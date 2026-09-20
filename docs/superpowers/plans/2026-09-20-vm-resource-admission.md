# Task D: Resource-based VM admission seams and implementation plan

**Status:** implementation under the owner-approved remaining roadmap. D1 resource/placement primitives (`c62177172`), D2 sanitized collector (`5b3219e07`) and immutable inventory store/migration 0263 (`24feabea8`) passed independent review. Authenticated ingestion, default-off observer runtime and shared Helm policy (`94b13145a`) passed independent review, 133 inventory/Helm checks and 23 startup/runtime compatibility tests. The endpoint inventory now recognizes the existing fixed-purpose publisher authentication gate (`c9a01ab52`) without changing permissions or configuration. Direct read-only collection on dedicated k3d returned complete eight-kind inventory; no live publication or admission is claimed. D3 exact occupancy (`bb1b26daa`, 24 tests), persistent fairness nomination (`7596fd415`, 12 tests), immutable reservation schema/evidence retention (`0e1ee5a88`, 24 PostgreSQL/head tests), and the unconnected atomic admission store (`e182b1e37`, 32 PostgreSQL/placement checks) passed independent review. Bounded invalid-head maintenance (`9b2c5fe00`, corrected by `be992cc33`) also passed review with 25 PostgreSQL maintenance checks: transient blockers park without losing age/protection, terminal no-effect requests can cancel, and malformed lineage quarantines only its own reservation-free waiter so unrelated owners can advance. These stores remain unconnected and all observer, shadow and enforcement modes remain off by default. Trusted waiter projection, runtime maintenance wiring, reservation binding/release, lifecycle integration, accounting visibility and enforcement remain incomplete. Production policy values and live acceptance remain rollout prerequisites.

The first trusted-template prerequisite is reviewed (`5bf49d0e7`, corrected by `d862054d8`): a pure bounded parser reads the original VM YAML without credential rendering and projects only supported CPU/memory, placement and ordinary root-storage semantics. Unsupported scheduling, devices, dynamic placement and hidden PV binding/population constraints are refused. All 59 parser/placement checks and three independent storage-constraint repros passed. This helper is not wired into admission: versioned controller configuration, trusted owner/project/priority projection, measured host-cost mapping, final rendered-manifest validation and reservation binding are still required.

**Snapshot inspected:** worktree HEAD `91f9971e14ade45d154bf8bff946dfaed3700b85` on 2026-09-20. The worktree also contained unrelated in-progress Task 3 changes; line numbers must be refreshed before implementation.

## Decision

Make PostgreSQL the authority that coordinates SRW VM compute reservations across orchestrator and controller replicas. Feed it a bounded, authenticated Kubernetes inventory from the same-cluster VM controller. Kubernetes remains the placement authority: a reservation only coordinates SRW demand and selects a candidate node; the scheduler still accepts or leaves the VMI Pending after evaluating current cluster state.

The first cohesive slice must reserve a per-node host resource vector before the first creation effect, bind the reservation to the immutable creation request, inject required hostname affinity for that node, prevent a second replica from spending the same capacity, and retain the reservation through pending, active, warm, and teardown states until exact physical compute absence is observed. It must also include owner-fair ordering, external scheduled Pod occupancy, stale-inventory refusal, operator/owner visibility, and a default-off Helm contract. Omitting any of those pieces produces either oversubscription, starvation, silent capacity loss, or early reuse during teardown.

Storage bytes remain a separate capacity problem. This slice considers retained-PV and StorageClass topology only to determine eligible nodes; it does not represent storage capacity as CPU or RAM.

## Current seams and their consequences

### Count gate

- `helm/values.yaml` sets `vmController.maxConcurrentVms: 4`; `helm/templates/vm-controller/deployment.yaml` publishes it as `VM_MAX_CONCURRENT`.
- `src/vm_controller/controller.py::_capacity_wait` lists namespaced KubeVirt `VirtualMachine` objects, counts `agent-vm-*`, excludes golden objects and every object with `deletionTimestamp`, and returns `waiting_capacity` when the count reaches the cap.
- `VMController._capacity_lock` is process-local. The chart currently uses one controller replica and `Recreate`, but the lock cannot coordinate future replicas or another writer.
- Both the legacy create path and `src/vm_controller/creation_actuation.py::CreationActuator.run` call the count gate. It knows neither VM size nor node fit. It also returns capacity as soon as deletion is requested, before the VMI or `virt-launcher` Pod is physically gone.
- Keep this gate as an independently named operational count backstop. Resource admission must not silently reinterpret `maxConcurrentVms`, and operators must explicitly raise or disable the backstop before proving the roadmap case of more than four small VMs.

### Immutable creation and effect authority

- `vm_creation_retries` (migrations 0257-0260) already provides an immutable `request_id`, Job ID, provision generation, canonical request, controller-configuration digest, retry state, and exact adoption identities.
- `vm_creation_effects` provides single-flight rootdisk, cloud-init, and VM effects. `VMCreationRetryStore.authorize_controller` currently composes the request with `vm_workspace_cleanup_admissions`, while `begin_effect` is the actual Kubernetes-effect grant.
- `VMCreationRetryService` is leader-gated, but the store is already written as PostgreSQL authority and must remain safe if callers or controller replicas overlap.
- The resource reservation must link to `vm_creation_retries.request_id`. It must not overload `vm_workspace_cleanup_admissions`: cleanup permits serialize owner/PVC mutation, while a compute reservation represents a per-node consumable vector with a different lifetime.
- `CreationActuator.run` currently evaluates `_capacity_wait` before its `/authorize` call. With resource enforcement on, `/authorize` must additionally return the exact reservation, and every `begin_effect` must independently require that reservation. A controller reply alone must never grant capacity.

### Request size and rendered VMI

- `src/orchestrator/services/vm_creation_request.py` freezes `cpu_cores` and Kubernetes memory quantity in the canonical request.
- `helm/templates/vm-controller/configmap.yaml` renders only `domain.cpu.cores` and `domain.memory.guest`; it does not set a host CPU request. `render_template` copies configured `nodeSelector` and tolerations, with no current node affinity.
- Guest values therefore cannot be subtracted directly from node allocatable resources. Admission needs an explicit, versioned host-cost policy. The policy must be operator-supplied when enforcement is enabled and stored with each reservation:
  - `guest_vcpus`
  - `guest_memory_bytes`
  - `host_cpu_millicores = ceil(guest_vcpus * cpuMillicoresPerVcpuNumerator / cpuMillicoresPerVcpuDenominator) + launcherCpuOverheadMillicores`
  - `host_memory_bytes = guest_memory_bytes + fixedMemoryOverheadBytes + perVcpuMemoryOverheadBytes * guest_vcpus + ceil(guest_memory_bytes * memoryOverheadBasisPoints / 10000)`
  - one `devices.kubevirt.io/kvm` device
- Do not hard-code a main-cluster ratio, overhead, headroom, or quota. The repository README's approximate KubeVirt launcher formula is useful for an operator worksheet and shadow comparison, but it is not a discovered production policy.
- Reuse the existing quantity and Pod normalization algorithms for Kubernetes effective CPU/RAM requests. The controller image has no orchestrator package, so the pure implementations move to `shared.kubernetes_quantities` and `shared.kubernetes_pod_requests`, with the original metering modules retaining compatibility exports. This implementation already covers normal containers, restartable/non-restartable init containers, Pod-level requests, Pod overhead, and in-place resize status. Add a focused integer extractor for `devices.kubevirt.io/kvm`; the existing normalizer deliberately handles only CPU and memory. Admission must not create a second, subtly different CPU/RAM request algorithm.

### Eligible-node fit and external occupancy

There is no present VM node-fit calculation. A candidate node must satisfy all of the following before its resources are considered:

1. Node UID and name are present; `Ready=True`; `spec.unschedulable` is false.
2. The existing VM `nodeSelector` matches.
3. Every `NoSchedule` and `NoExecute` taint is tolerated by the VM's configured tolerations. `PreferNoSchedule` affects ranking, not hard eligibility.
4. `status.allocatable[devices.kubevirt.io/kvm]` has an unconsumed device.
5. A retained PV's required node affinity permits the node. For a new disk, the StorageClass topology/volume-binding behavior is recorded; a later bound PV that conflicts with the reserved node blocks the VM effect and triggers the pre-VM re-selection protocol described below.
6. The per-resource available vector fits after external occupancy, managed VM charges, unbound reservations, and operator headroom are deducted.

The VM controller is the narrowest existing component that already reads KubeVirt objects, namespace Pods/PVCs, and exact Node identity and already holds the lifecycle HMAC used for calls back to the orchestrator. Add a bounded inventory publisher there instead of giving every orchestrator replica broad Kubernetes credentials.

The observer requires an explicit cluster-wide read acknowledgement because exact external occupancy needs Pods in every namespace. Its ClusterRole is read-only and limited to:

- `nodes`: `list` (the existing separate exact-identity role retains `get`)
- `pods`: `list`
- `persistentvolumes` and `storageclasses`: `list` for topology only
- the existing namespaced VM/VMI/PVC/DataVolume reads

The published document is sanitized: node UID/name, allowlisted placement labels, taints/conditions, allocatable CPU/RAM/KVM, normalized Pod UID/node/effective request/terminal/deleting state, VM/VMI/launcher public identity and owner-generation labels, and PV/StorageClass topology. It must exclude environment, commands, images, Secret references, raw annotations, raw status messages, and API exception text. Cap item count and encoded bytes; any pagination, normalization, watch-gap, or truncation failure marks the snapshot incomplete.

Scheduled nonterminal Pods, including deleting Pods, consume their effective requests until terminal state or exact absence. Terminal Pods do not. Unscheduled external Pods are surfaced as pending demand but have no node to debit; the API must state that concurrent non-SRW scheduling can still win a race and Kubernetes is final authority. Admission must never claim cluster-wide exclusivity against external writers.

### No double counting

For each node, calculate:

```text
available = allocatable
          - operator_headroom
          - external_scheduled_pod_requests
          - managed_bound_vm_charges
          - managed_unbound_reservations
```

Classification is by exact Pod UID/owner reference plus the reservation ID and generation annotations, never by a reusable name alone.

- An SRW reservation without an exact scheduled launcher Pod is `unbound` and is charged from its stored host vector.
- Once the exact launcher Pod is scheduled on the reserved Node UID, charge the component-wise maximum of the reservation vector and the normalized launcher Pod effective request. Do not also subtract that Pod as external occupancy.
- A launcher Pod with no trusted reservation is external occupancy during dark launch. Before enforcement cutover, create a synthetic imported reservation for every attributable live SRW VMI; ambiguous objects keep enforcement blocked.
- KubeVirt importer, CDI, platform, and unrelated workload Pods are ordinary external occupancy.
- KubeVirt control-plane overhead is therefore included through its scheduled Pod requests. Per-VM launcher overhead remains part of the reservation formula and is compared with observed launcher requests in shadow telemetry.

### Queue, lifecycle, and release

`get_admittable_stateless_jobs` currently orders Jobs by priority then creation time. `run_queue` has a user-derived `fair_key`, but VM creation happens before worker-queue admission, so that queue cannot be the VM capacity authority.

Create a durable VM-specific wait row as part of creation retry admission. Its owner key is the Job `user_id` when present, otherwise a stable system-owner key; project ID is stored for future quota/reporting but does not replace the owner key. Within an owner, preserve priority and enqueue time. Across owners, use a durable round-robin sequence:

1. Consider one head request per owner using an aged effective priority: base Job priority plus a wait-age increment. The aging interval is explicit configuration, so a continuing stream of newer high-priority requests cannot starve a finite older request.
2. Order owner heads by effective priority, then least-recently-admitted owner sequence, then enqueue time and request UUID.
3. Permanently nonfit requests (larger than every statically eligible node after headroom, independent of current occupancy) become `nonfit` and do not block other owners. They remain visible for resize/cancel/operator action.
4. Temporarily nonfitting owner heads can be bypassed only up to an explicitly configured `maxBypasses`. At the limit they become protected, preventing unlimited small-request backfill. Do not invent the production value; require an explicit value when enforcement is enabled. `0` gives strict no-bypass behavior.
5. Update the global admission sequence and owner's last-admitted sequence in the same transaction that inserts the reservation.

Protection is persisted on the skipped waiter and survives new priorities or a different ordinary head for the same owner. Protected heads precede bypassable work, ordered by persisted protection sequence, enqueue time and request UUID. `maxBypasses: 0` protects the first temporarily blocked eligible head immediately. Increment bypass counts only in a committed later reservation, never for polling, CAS loss, inventory refusal or nonfit classification. Global/owner fairness sequence advances in that same commit.

Select a candidate outside its authority transaction, then acquire only that request's established Job/retry locks before cluster policy and inventory-head locks. Recompute the winner after waiting. A different winner returns a nomination; end the transaction and acquire the nominee's normal authority on the next attempt. Never lock a foreign Job after policy. Cancelled/expired/recovery-held heads require bounded maintenance under their own authority; use persisted cursor progress so a prefix of invalid heads cannot hide valid work across replicas/restarts.

`nonfit` requires complete supported static capacity evidence and size failure independent of occupancy. Ready/cordon/device advertisement changes, unknown label coverage and unbound topology are waits, not permanent nonfit. Reconsider nonfit under newer relevant inventory/policy while preserving enqueue identity, priority aging and prior fairness history.

Optional hard owner CPU/RAM budgets are distinct from fairness. An absent owner budget means no separate hard quota; it does not disable global fit. If budgets are configured later, persist a versioned policy and compare active plus held reservations atomically.

Reservation states and their capacity behavior are:

| State | Meaning | Capacity charge |
|---|---|---|
| `waiting` | Immutable request is queued but no node is reserved | None |
| `reserved` | Node selected; no exact launcher is scheduled | Stored host vector |
| `active` | Exact VMI/launcher is present on the selected Node UID | `max(reservation, launcher request)` |
| `warm` | Future idle-but-running retained VM | Same as active |
| `teardown` | Delete/control is in progress or physical state is uncertain | Same as reserved/active |
| `released` | Exact VM, VMI, and launcher identities are physically absent | None; historical row retained |
| `nonfit` | Request cannot fit any statically eligible node under current policy | None |

Cancellation may release a compute reservation when the effect ledger proves that the VM effect was never issued; already-created disk or Secret objects retain their separate cleanup authority but do not consume compute. Once a VM effect could have issued, a TTL, claim expiry, Job terminal state, VM `deletionTimestamp`, or successful delete request is never sufficient to return compute.

Reuse the exact teardown evidence in `VMProvisioner.capture_vm_teardown_identity`, `_probe_vm_teardown_identity(exact_absence=True)`, and `delete_vm_captured`. The capacity reconciler releases only after the expected VM UID, VMI UID, and launcher Pod UID are absent and no replacement of the same generation exists. Retained PVC/DV state is separate: a retained disk does not hold compute after exact VM/VMI/launcher absence.

If a new disk binds to topology incompatible with the reserved node, re-selection is allowed only while the effect ledger proves no VM effect was issued. Mark the old reservation released with reason `storage_topology_changed`, preserve the disk identity, and enqueue a successor reservation revision for the same request. After a VM effect is issued, node/reservation identity is immutable and topology drift is operator attention.

The minimum slice intentionally pins the initial VMI to one selected Node UID/name through required hostname affinity. Helm enforcement must refuse a configuration that enables automatic cross-node live migration or another relocation policy until a transfer protocol exists. That later protocol must reserve the target while retaining the full source charge, allow the controlled affinity change, observe the exact target launcher, and release the source only after its launcher is absent. Node drain can therefore leave a minimum-slice VM Pending on its original node; this is a known availability tradeoff, not permission to move its reservation by name.

## Database model and lock order

Use the next app migration number available at implementation time. Committed local head is 0268; A1 owns the next 0269 migration for typed cancellation completion. Update `src/orchestrator/database/schema_current.sql` and the migration-head checks in the same change.

Suggested tables:

- `vm_resource_inventory_snapshots`: snapshot UUID, cluster ID, controller instance, observed time, node/pod resourceVersions, complete flag, digest, bounded sanitized JSON, policy digest, received time. Keep bounded history, a persisted observation high-water mark and one current observation pointer, including incomplete/stale observations. Never fall back to older complete capacity. Identical retained snapshots return the original immutable receipt; after history pruning, older IDs are refused by the high-water mark. D3 must protect every reservation-referenced snapshot from pruning.
- `vm_resource_waiters`: one row per retry request; request/Job/generation FK identity, owner/project keys, priority, enqueue time, immutable guest and host vectors, policy digest, state/reason, bypass count/protection time, revision.
- `vm_resource_reservations`: reservation UUID, request ID unique, selected node UID/name, immutable host vector and KVM count, policy/snapshot digests, lifecycle state, exact VMI/launcher identities when learned, teardown/release evidence and timestamps.
- `vm_resource_admission_policy`: singleton cluster/policy revision, mode, current inventory pointer, global admission sequence and rollout state.
- `vm_resource_owner_fairness`: owner key, last admitted sequence, active reserved CPU/RAM/KVM totals, optional quota-policy reference.

Use integer millicores and bytes with nonnegative/bounded checks. Store Kubernetes quantity source strings only in sanitized evidence, not as arithmetic authority. Add unique constraints for one waiter/reservation per retry request and immutable triggers for request, generation, node UID/name after VM-effect issue, resource vector, and policy digest.

All resource admission/reconciliation paths use this lock order (D3 must lock the current inventory-head row together with the resource policy in one consistent order):

1. Existing owner/PVC advisory locks and cleanup/recovery locks when the operation also changes Job or retry authority.
2. Job, retry, and effect rows using the existing `_scope` order.
3. One cluster-wide resource policy row (key excludes namespace/policy digest); all earlier-policy held vectors remain charged unchanged.
4. Candidate node ledger rows ordered by Node UID.
5. Waiter, reservation, and owner-fairness rows ordered by UUID/owner key.

Inventory publication never locks Job rows. It stores an immutable complete or incomplete observation, then advances the current pointer under the inventory-head lock. Collection freshness starts at the earliest LIST, and receipt freshness uses database time sampled after that lock. Equal observation timestamps with different identities invalidate availability; future observation time is refused. Neither transport re-signing nor replay refreshes an old receipt. Admission never performs Kubernetes I/O inside a database transaction.

## Service boundaries

Add these focused modules rather than growing `vm_creation_retry_store.py` further:

- `src/shared/vm_resource_admission.py`: typed resource vectors, canonical policy/snapshot/reservation digests, sanitized wire validation, and reason enums.
- `src/vm_controller/resource_inventory.py`: bounded paginated LIST collection, node/Pod/VMI/PV normalization, complete/incomplete observations, and authenticated publication. Watch is deferred until explicit gap invalidation exists.
- `src/orchestrator/services/vm_resource_admission_store.py`: short PostgreSQL transactions for waiter creation, atomic fit/fairness selection, reservation lookup, bind/teardown/release reconciliation, and admin projection.
- `src/orchestrator/services/vm_resource_admission.py`: leader loop; no network calls while store transactions are open.
- `src/orchestrator/routers/vm_resource_inventory.py`: lifecycle-HMAC authenticated snapshot ingestion. Reuse the shared HMAC primitive with operation `vm_resource_inventory_publish`; read the bounded request stream before JSON decoding, reject duplicate keys/deep or ambiguous JSON, and return a signed correlated receipt. The existing unbounded request.json authentication helper is unsuitable for this endpoint.

Integration changes:

- `VMCreationRetryStore.admit_on_conn`: after the resolved canonical request and Job owner are locked, insert/idempotently validate the wait row. With the feature off, record shadow input only if shadow is enabled; do not block creation.
- `VMCreationRetryStore.authorize_controller`: when enforcement is on, require an active reservation for the exact request/generation/policy, return reservation ID/node UID/name/vector/digests, and then compose the existing cleanup permit. Replays return the same reservation.
- `VMCreationRetryStore.begin_effect`: independently refuse every fresh effect if enforcement is on and the exact reservation is absent, released, stale-policy, or has mismatched carrier identity. This protects against a stale controller response.
- `CreationActuator.values` and carrier intent: bind reservation ID, node UID/name, policy digest, snapshot digest, and resource vector. Those fields become immutable effect authority.
- `CreationActuator.body`: merge required node affinity for the selected hostname with existing placement constraints. Re-read the node and require the same UID immediately before every fresh Kubernetes POST. Never use `spec.nodeName`.
- `src/vm_controller/creation_configuration.py`: include the normalized resource-admission policy and placement-render algorithm version in the controller configuration digest.
- `VMCreationRetryService.reconcile_once`: run resource admission/reconciliation before claiming creates, or as a sibling leader service with a deterministic ordering. A request without a reservation returns durable `resource_wait`, not a transport outage and not a boot attempt.
- Teardown/recovery services: mark reservations `teardown` when exact cleanup authority is admitted; call release reconciliation with captured exact-absence evidence. Never release from a context-only status update.
- `src/orchestrator/services/stateless_capacity.py` and `src/orchestrator/routers/capacity.py`: extend the existing admin response with a separate `vm` block. Do not mix VM resource vectors into KEDA executor `desired`.

## Helm contract

Place one shared policy under `vm.resourceAdmission` so the orchestrator and VM controller render from the same values. Keep all behavior off by default:

```yaml
vm:
  resourceAdmission:
    observerEnabled: false
    shadowEnabled: false
    enforcementEnabled: false
    clusterWidePodReadAcknowledged: false
    stableClusterId: ""
    inventory:
      publishIntervalSeconds: null
      staleAfterSeconds: null
      maxItems: null
      maxBytes: null
      requestTimeoutSeconds: null
      collectionTimeoutSeconds: null
      publicationTimeoutSeconds: null
      historyLimit: null
      nodeLabelKeys: [] # explicitly include hostname and every placement/topology key
    hostCost:
      cpuMillicoresPerVcpuNumerator: null
      cpuMillicoresPerVcpuDenominator: null
      launcherCpuOverheadMillicores: null
      fixedMemoryOverheadBytes: null
      perVcpuMemoryOverheadBytes: null
      memoryOverheadBasisPoints: null
    nodeHeadroom:
      cpuMillicores: null
      memoryBytes: null
      kvmDevices: null
    fairness:
      maxBypasses: null
      priorityAgingSeconds: null
```

Schema/template rules:

- `enforcementEnabled` requires `observerEnabled`, `shadowEnabled`, `creationRetryEnabled`, same-cluster VM mode, lifecycle HMAC, stable cluster ID, the explicit cluster-wide Pod-read acknowledgement, and every numeric policy field.
- `shadowEnabled` requires the observer but does not influence admission, creation, or deletion.
- No default production CPU/RAM ratio, overhead, headroom, quota, freshness, bypass, or priority-aging value is supplied. Test overlays provide explicit small values.
- `vmController.maxConcurrentVms` remains the count backstop and gets clearer documentation. It is neither derived from nor automatically disabled by resource admission.
- Hash the complete policy into both Pod templates. Enforced policy changes use `Recreate` for the orchestrator as well as the existing controller `Recreate`, so mixed policy writers cannot overlap.
- Emit cluster-wide RBAC only when the observer is enabled and acknowledged. Helm rendering must fail if broad read authority would be enabled implicitly.
- NetworkPolicies need no new public ingress. Controller-to-orchestrator publication uses the existing internal authenticated path.

Rollout gates must be separate:

1. `off`: no observer, no database effect.
2. `observer`: publish sanitized inventory; no waiter/reservation effect.
3. `shadow`: compute decisions and compare reservation vectors with actual launcher requests; no create is delayed.
4. `enforce`: only after current snapshot health, imported-live-VM coverage, policy agreement, and disposable KubeVirt acceptance pass.

Rollback from enforcement cannot simply turn the gate off while held reservations or new-protocol effects exist. A rollback mode must keep reconciliation/release active while blocking new resource admissions, matching the existing rule that disabled creation retry still reconciles existing intent.

## Visibility and accounting

Owner-visible Job/VM projection should expose only the owner's request and progress:

- `waiting_resource`, `reserved`, `starting`, `active`, `tearing_down`, `released`, or `nonfit`
- requested guest CPU/RAM and stable request ID
- sanitized reason such as `inventory_stale`, `no_eligible_node`, `cpu_wait`, `memory_wait`, `kvm_wait`, `owner_budget`, or `storage_topology_wait`
- whether a boot attempt has started (resource wait does not increment it)
- cancel/retry guidance; retry of the same generation preserves the original enqueue time

Do not expose other owners, exact queue rank, node names, or cluster totals to a regular owner.

Extend `GET /api/admin/capacity` and the existing Cockpit Admin Capacity view with:

- inventory observed time/age/completeness and policy digest
- eligible/ineligible node counts and sanitized exclusion reasons
- allocatable, headroom, external scheduled, managed active/warm, unbound reserved, teardown-held, and available CPU/RAM/KVM totals
- waiting/nonfit counts, oldest wait, owner fairness activity, and bypass/protected counts
- count-backstop current/max as a separate field
- reservations overdue for exact teardown reconciliation

Prometheus/audit signals should include admission decision counts by sanitized reason, wait duration, snapshot age/failure, reserved/active/teardown vectors, node-fit rejection, fairness bypass/protection, release latency after physical absence, and count-backstop blocks.

Scheduling reservations are not automatically billable usage. Existing infrastructure metering continues to derive usage from observed VMI/Pod intervals. Persist reservation timestamps and vectors for audit, but any future charge for held capacity requires a separate pricing/product decision.

## Implementation order and tests

### 1. Pure resource and placement model

**Files:** add `src/shared/vm_resource_admission.py`, `vm_resource_placement.py` and corresponding tests; extract the pure shared normalizers with compatibility exports.

Current local foundation covers arithmetic, explicit policy digests, shared Pod requests, whole KVM devices, deleting/terminal/unscheduled occupancy and static placement. The component-wise managed-charge helper requires caller-proven launcher identity; complete inventory classification and prevention of double counting remain stages 2-3.

- Write RED tests for CPU rounding, memory overhead components, quantity rejection, vector add/subtract/max, KVM devices, selector matching, taints/tolerations, Ready/cordon exclusion, retained-PV node affinity, and deterministic digests.
- Reuse the existing Pod normalizer and prove init-container, Pod-overhead, resizing, terminating, and terminal cases contribute correctly.
- Test that an SRW launcher is charged once as `max(reservation, observed request)` and that an unbound reservation is charged without a Pod.

### 2. Inventory observer and authenticated ingestion

**Files:** add `src/vm_controller/resource_inventory.py`, `src/orchestrator/routers/vm_resource_inventory.py`, focused router/controller tests; wire `src/vm_controller/controller.py` startup and `src/orchestrator/main.py` router configuration.

- RED: incomplete page, watch gap, invalid quantity, duplicate UID, item/byte overflow, stale node UID, and API error all publish an incomplete snapshot that cannot authorize admission.
- RED: raw environment, command, image, Secret reference, annotation, and error text cannot cross the wire.
- RED: invalid MAC, replayed request, wrong cluster ID, nonmonotonic observation, or digest mismatch is rejected.
- Publish/store outside admission transactions; cancellation cleanly stops the observer.

### 3. Durable waiters and atomic reservations

Before introducing reservation FKs, extend D2 pruning with a nonlocking reference exclusion and retain restrictive FKs. Referenced history is additional to the bounded unreferenced receipt window.

**Files:** add the next migration (next free number after current inventory migration 0263; coordinate with A1 and do not reuse its number), update `schema_current.sql` and migration checks, add `vm_resource_admission_store.py`, and add `tests/test_vm_resource_admission_real_postgres.py`.

- RED with actual PostgreSQL: two concurrent replicas compete for the final fitting vector; exactly one reservation commits.
- RED: repeat admission of one request returns the same reservation and does not change totals.
- RED: CPU-fit/RAM-nonfit, RAM-fit/CPU-nonfit, KVM exhaustion, stale/incomplete inventory, node UID replacement, and low-count oversized request all refuse.
- RED: scheduled external Pod requests reduce fit; deleting nonterminal Pods still count; terminal/absent Pods stop counting.
- RED: multiple owners round-robin, one owner's aged priority ordering is preserved, a lower-priority request eventually advances under a sustained higher-priority arrival stream, impossible requests do not block, and bounded bypass eventually protects a temporarily blocked head.
- RED: optional owner quota and global fit are independent.
- Exercise the declared lock order under concurrent admit/cancel/teardown/inventory transactions and fail the test on deadlock or double release.

### 4. Bind reservations to creation effects and placement

**Files:** modify `vm_creation_retry_store.py`, `vm_creation_retry.py`, `vm_creation_issuance.py`, `vm_creation_transport.py`, `vm_controller/creation_actuation.py`, `vm_controller/creation_configuration.py`, and their focused unit/real-PG suites.

- RED: enforcement refuses authorize and begin-effect without the exact active reservation.
- RED: lost authorize/begin-effect replies and replica retries reuse one reservation and one effect nonce.
- RED: wrong node UID/name, vector, policy digest, snapshot digest, request, or generation refuses before POST.
- RED: rendered VMI contains merged required hostname affinity and preserves selector/tolerations; node UID drift immediately before POST refuses.
- RED: a bound-PV topology conflict can reselect only before a VM effect; after VM issuance it becomes attention.
- Preserve legacy behavior byte-for-byte while enforcement is off.

### 5. Reconcile active, warm, and teardown accounting

**Files:** extend the resource service/store and connect exact teardown paths in `vm_provisioner.py` and lifecycle/recovery services; add real-PG and controller-fixture tests.

- RED: exact VMI/launcher observation moves reserved to active and prevents double counting.
- RED: a future warm flag retains the active charge.
- RED: Job completion, delete acceptance, VM `deletionTimestamp`, claim expiry, and reservation TTL do not release capacity.
- RED: exact expected VM/VMI/launcher absence releases once; replacement UID or uncertain API keeps teardown held.
- RED: retained PVC/DV survives while compute reservation releases after physical stop.
- Add an overdue teardown admin projection and metric.

### 6. Helm and API/UI contracts

**Files:** update `helm/values.yaml`, `helm/values.schema.json`, controller/orchestrator deployments, controller RBAC, template checksum/strategy gates, README; add `tests/test_helm_vm_resource_admission.py`; extend capacity router/service and Cockpit capacity model/component tests.

Helm RED cases:

- defaults render resource admission fully off and add no cluster-wide Pod/PV read RBAC
- shadow/enforcement dependency combinations fail clearly
- enforcement without explicit host-cost, headroom, freshness, fairness, stable cluster ID, HMAC, or RBAC acknowledgement fails
- observer-on emits only required read verbs and publishes identical policy to controller/orchestrator
- enforcement refuses automatic live-migration/relocation configuration until target-reservation handoff is implemented
- `maxConcurrentVms` remains independently configurable and documented as a count backstop
- a policy change alters both Pod-template checksums and selects non-overlapping rollout behavior

API/UI tests must prove owner responses exclude fleet details while admin output accounts for every vector exactly once and reports stale/incomplete inventory.

### 7. Bounded acceptance

First run unit, Helm, and actual-PostgreSQL concurrency suites. Then use the dedicated disposable k3d/KubeVirt cluster with explicit test-only policy values:

1. External scheduled Pod occupancy reduces available CPU/RAM.
2. Parallel create requests that together exceed a node result in one reservation and one durable waiter, not two VM effects.
3. A low-count VM too large for every node becomes `nonfit`.
4. More than four small VMs can be admitted only when measured fit allows it and the test overlay explicitly raises/disables the count backstop.
5. Deleting a VM retains its capacity through VM deletion request and VMI teardown, then releases only after launcher absence.
6. Restart orchestrator/controller between reserve, effect, bind, and teardown checkpoints; identities and totals remain stable.
7. Two owners demonstrate round-robin progress and bypass protection.

The local-path result proves CPU/RAM/KVM admission, cross-replica PostgreSQL serialization, placement affinity, and exact compute release. It is not the Longhorn acceptance gate. Final release still needs the same matrix on the intended Kubernetes/KubeVirt/Longhorn topology with real inventory-derived policy values and no guessed production ceilings.

## Minimum cohesive slice acceptance checklist

- [x] Default chart values leave observer, shadow, and enforcement off; unfinished shadow/enforcement modes currently refuse enablement.
- [ ] Enabling enforcement requires explicit operator policy and cluster-wide read acknowledgement.
- [ ] Complete, fresh, sanitized inventory includes eligible nodes and all scheduled external Pod requests.
- [ ] One PostgreSQL transaction selects an owner-fair waiter and inserts one per-node reservation.
- [ ] Two replicas cannot reserve the same final CPU/RAM/KVM capacity.
- [ ] The immutable creation/effect carrier binds the reservation and selected Node UID.
- [ ] The VMI uses required hostname affinity and Kubernetes remains final placement authority.
- [ ] Pending, active, warm, and teardown compute is accounted once.
- [ ] Capacity returns only after exact VM/VMI/launcher physical absence.
- [ ] Oversized low-count requests refuse; fitting requests can exceed four only after an explicit count-backstop change.
- [ ] Owner projection explains waiting/retry/cancel without disclosing fleet state.
- [ ] Admin projection shows reserved, available, external, and overdue teardown vectors with snapshot freshness.
- [ ] Actual-PostgreSQL race/fairness/release tests and disposable KubeVirt acceptance pass.
- [ ] Longhorn validation and production policy values remain explicit rollout prerequisites.
