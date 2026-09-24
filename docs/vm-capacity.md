# VM capacity diagnostics

Administrators can read VM resource accounting in the `vm` block of
`GET /api/admin/capacity`. The executor counts and KEDA `desired` value retain
their existing meaning. This endpoint does not reserve resources or enable
resource admission.

## Configuration and rollout boundary

The chart currently permits resource observation only. It rejects
`vm.resourceAdmission.shadowEnabled: true` and
`vm.resourceAdmission.enforcementEnabled: true` while runtime qualification is
unfinished. Both default to `false`, as does `observerEnabled`. The presence of
capacity tables or reservation records does not enable enforcement.

Configure observation under `vm.resourceAdmission` in your Helm values:

| Setting | Purpose |
| --- | --- |
| `observerEnabled` | Publish authenticated inventory; defaults to `false`. |
| `stableClusterId` | Stable identity for the cluster's policy and inventory. |
| `clusterWidePodReadAcknowledged` | Explicitly permit the observer's cluster-wide Pod inventory. |
| `inventory` | Explicit freshness, timeout, item/byte/history bounds and placement label keys. |
| `launcherProfile` | Expected installed KubeVirt launcher shape; requires the exact KubeVirt namespace/name and architecture label. |
| `hostCost`, `nodeHeadroom`, `installationBudget`, `ownerBudget`, `fairness` | Operator-supplied admission policy inputs; left unset by default. They do not bypass the chart's enforcement gate. |

Observation requires `vm.mode: same-cluster`, a configured lifecycle HMAC
Secret, and all required inventory settings. See the comments in
[the chart values](../helm/values.yaml) for the complete fields. Set resource
budgets from the actual launcher requests and node overhead; guest CPU and
memory alone do not describe host demand.

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
