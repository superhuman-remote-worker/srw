# VM capacity diagnostics

Administrators can read VM resource accounting in the `vm` block of
`GET /api/admin/capacity`. The executor counts and KEDA `desired` value retain
their existing meaning. This endpoint does not reserve resources or enable
resource admission.

Each installed cluster policy reports its mode and inventory freshness. The
projection reads policy, inventory, waiters and reservations in one read-only
database snapshot. `available` means that inventory can be accounted for; it
does not promise that a particular VM can schedule.

Resource vectors contain CPU millicores, memory bytes, ephemeral-storage bytes,
and KVM, TUN and vhost-net device counts. Node totals distinguish allocatable
resources, headroom, scheduled external demand, unbound reservations, active and
warm VMs, teardown holds, remaining resources and shortfalls. Request selectors,
tolerations, storage topology and individual VM size still determine placement.

The separate `held` block retains known durable charges when inventory is
missing, incomplete, stale or unclassified, and when policy mode is `off` or
`drain`. Unknown dimensions are `null`. `orphaned_held` accounts for reservations
whose exact node disappeared; these are separate from the node totals. Pending
external workloads are also reported separately from scheduled demand. An exact
managed launcher, including an authenticated recovery successor, is not charged
again as external demand.

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
