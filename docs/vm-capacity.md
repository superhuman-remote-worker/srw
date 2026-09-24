# VM capacity diagnostics

Administrators can read VM resource accounting in the `vm` block of
`GET /api/admin/capacity`. The executor counts and KEDA `desired` value retain
their existing meaning. This endpoint does not reserve resources or enable
resource admission.

## Configuration and rollout boundary

Resource admission remains disabled by default. Observation alone uses
`observerEnabled: true` with `shadowEnabled` and `enforcementEnabled` false.
The full resource policy requires all three flags true, explicit
`clusterWidePodReadAcknowledged: true`, and
`orchestrator.vmProvisioning.creationRetryEnabled: true`. The flags install
one versioned policy document in both services; they do not activate the
durable admission mode automatically. A shadow-only flag combination is
unsupported. **Shadow is the durable policy's installation and audit state:**
while the full policy is deployed in that state, new VM creation waits for
activation. Capacity tables or reservation records alone grant no authority.

Configure observation under `vm.resourceAdmission` in your Helm values:

| Setting | Purpose |
| --- | --- |
| `observerEnabled` | Publish authenticated inventory; defaults to `false`. |
| `stableClusterId` | Stable identity for the cluster's policy and inventory. |
| `clusterWidePodReadAcknowledged` | Explicitly permit the observer's cluster-wide Pod inventory. |
| `inventory` | Explicit freshness, timeout, item/byte/history bounds and placement label keys. |
| `launcherProfile` | Expected installed KubeVirt launcher shape; requires the exact KubeVirt namespace/name and architecture label. |
| `hostCost`, `nodeHeadroom`, `installationBudget`, `ownerBudget`, `fairness` | Explicit six-dimensional admission inputs; left unset by default. The full policy cannot render with missing values. |

Observation requires `vm.mode: same-cluster`, a configured lifecycle HMAC
Secret, and all required inventory settings. See the comments in
[the chart values](../helm/values.yaml) for the complete fields. Set resource
budgets from the actual launcher requests and node overhead; guest CPU and
memory alone do not describe host demand.

Choose the installation and owner budgets from eligible-node capacity and
other workload reservations. The chart does not derive them. Review the exact
JSON rendered as `VM_RESOURCE_ADMISSION_CONFIG` in both Deployments; it must
have one policy digest and match the supported KubeVirt installation and
launcher profile. Use the same document for each operator transition. Install
the new application code and required migrations while resource admission is
still disabled, then use the configured orchestrator environment to run the
explicit policy command. Its input is a local file containing that reviewed
JSON policy, plus the expected stable cluster ID and policy digest:

```text
python -m orchestrator.operator_cli.vm_resource_policy ensure-shadow \
  --policy-file resource-policy.json --cluster-id CLUSTER_ID \
  --policy-digest sha256:POLICY_DIGEST
```

The command emits a JSON receipt with `cluster_id`, `namespace`,
`policy_digest`, `revision`, and `mode`. Save the complete receipt. Every later
transition requires it through `--expected-receipt-file`; a changed policy or
stale receipt is refused. The command never selects budgets, changes Helm
flags, or starts admission in the background.

```text
python -m orchestrator.operator_cli.vm_resource_policy activate-enforce \
  --policy-file resource-policy.json --cluster-id CLUSTER_ID \
  --policy-digest sha256:POLICY_DIGEST \
  --expected-receipt-file shadow-receipt.json
```

Use the new receipt from each successful transition for `begin-drain` and
`finalize-off`, with the same policy file, cluster ID and digest.

After installing the shadow row, deploy the **same** reviewed all-true policy
to both services with durable creation retry enabled. New creates hold while
the durable mode is `shadow`. Confirm a fresh, complete, authenticated
whole-cluster inventory, the exact installed launcher profile, and the
absence of any SRW-attributable VM, VMI or launcher and any unresolved
reservation.
Then invoke `activate-enforce` with the saved shadow receipt and save its new
receipt. The service checks those conditions again under the policy lock.
Genuine Job and pinned-session creates then use the same durable reservation
ledger; Kubernetes still makes the final placement decision. Validate both
owner paths and physical release before treating a live cluster as qualified.

To turn enforcement off, invoke `begin-drain` with the enforce receipt first.
This stops fresh grants while existing v3 effects and charged cleanup continue
to reconcile. **Keep the original all-true Helm policy and inventory publisher
in place during drain.** Wait for reservations and live waiters to settle and
for the fresh inventory to show no attributable SRW runtime, then invoke
`finalize-off` with the drain receipt. Only after its off receipt may the Helm
flags be returned to observer-only or disabled. Turning the process flag off
while the durable row still says `enforce` does not revoke already frozen v3
requests; changing the policy document during drain also removes the fresh
inventory needed to finish it. Do not roll back to an image that cannot
reconcile the existing reservation ledger.

The separate object-count backstop is `vmController.maxConcurrentVms`, passed
to the controller as `VM_MAX_CONCURRENT`. Its chart default is `4`; a deployment
may override it. It is not a resource-based safe-concurrency recommendation.
The admin endpoint reports its maximum as unknown until it has authoritative
controller configuration, even when this Helm value is configured.

## Reading capacity

In Cockpit, open **Admin → Capacity → VM capacity**. Each installed cluster has
its own policy mode, inventory freshness, count backstop, waiting and teardown
diagnostics, durable holds, and six-dimensional resource tables. Expand **Nodes**
with a click or Enter/Space to inspect exact node accounting. A dash means
unknown, including a missing count maximum or an unaccountable resource
dimension; it must not be read as zero. When inventory accounting is unavailable,
the page keeps known durable holds visible and hides node and cluster totals.

Each installed cluster policy reports its mode and inventory freshness. The
projection reads policy, inventory, waiters and reservations in one read-only
database snapshot. `available` means that inventory can be accounted for; it
does not promise that a particular VM can schedule.

Resource vectors contain CPU millicores, memory bytes, ephemeral-storage bytes,
and KVM, TUN and vhost-net device counts. Node totals distinguish allocatable
resources, headroom, scheduled external demand, unbound reservations, bound
reservations awaiting Ready (`bound_reserved`), active and warm VMs, teardown
holds, remaining resources and shortfalls. A bound reservation retains its
observed high-water demand even when that demand exceeds the admitted budget
and prevents Ready. Request selectors,
tolerations, storage topology and individual VM size still determine placement.

The separate `held` block retains known durable charges when inventory is
missing, incomplete, stale or unclassified, and when policy mode is `off` or
`drain`. Unknown dimensions are `null`. `orphaned_held` accounts for reservations
whose exact node disappeared; these are separate from the node totals. Pending
external workloads are also reported separately from scheduled demand. An exact
managed launcher, including an authenticated recovery successor, is not charged
again as external demand.

A legacy `agent-vm-` guest without sufficient owner or reservation identity
makes capacity unknown, including while deleting or without a launcher. Its
absence from Pod requests cannot establish free installation or owner capacity.
Golden-image VMs and unrelated guests keep their separate external accounting.

The count backstop is separate from resource accounting. `observed` counts
non-deleting `agent-vm-` objects, excluding golden-image VMs. Its configured
`maximum` remains unknown when no authoritative controller limit is available;
the endpoint does not infer that limit from reservations or local defaults.

Waiting diagnostics include original age, nonfit count, bypasses and protected
requests. They provide no completion estimate or exact queue rank. Teardown
diagnostics mark an exact idle-release operation overdue after five minutes
without progress. Holds without a matching operation have unknown age. This
threshold never expires a reservation: release still requires the normal
identity and physical-cleanup proofs.

The Job list and detail view, and the Sessions list, show the current owner's
creation wait when the request can be tied to that exact Job or pinned session
runtime. The owner view distinguishes a local resource wait, a controller count
hold, a controller connection hold, and an older or otherwise ambiguous hold.
For a local resource wait it may show the originally requested guest vCPUs and
memory, the original enqueue time, and an exact request-size nonfit finding.
It does not expose cluster budgets, other owners' requests, queue position,
predicted fit, or an ETA. A count maximum absent from the admin API remains
unknown in Cockpit too.
