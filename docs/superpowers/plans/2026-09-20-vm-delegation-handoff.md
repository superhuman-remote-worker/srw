# VM reliability: handoff to job-based development

Updated 2026-09-20. This is the entry point after conversation compaction and for
SRW development jobs. The owner wants bounded jobs using an economical configured
model, with this session coordinating and reviewing. Start with one VM job;
consider two only after the first succeeds. Do not restart the broad implementation
effort or previously paused agents automatically.

## Published and verified

- Application repository: `superhuman-remote-worker/srw`, branch `develop`.
  Reviewed implementation through `83ac143f0` was integrated in `7611c36a1`;
  fixture-only CI correction is `11a33debf`.
- [Release scope and validation](2026-09-20-vm-reviewed-checkpoint.md).
  [Develop CI](https://github.com/superhuman-remote-worker/srw/actions/runs/35526136186)
  succeeded at `11a33debf`, including test-python and deploy-experimental.
  The rerun's affected-test selection had 650 passes and two skips; all 124 tests in the three
  corrected files passed locally. The initial full CI run had 33,964 passes,
  12 fixture failures and 181 skips. A fresh full-suite green run is not claimed.
- Schema head is `0268_workspace_idle_terminal_exit.sql`. Do not introduce reads
  of `cancellation_completion` before the unfinished 0269 migration ships.
- Main-dev deployed images, flags, schema and VM readiness must be rechecked
  before scheduling the pilot; successful CI does not verify live execution.

## Remaining roadmap

### Login outage resolved after publication — 2026-09-20

Gitea's 8Gi PVC filled (20KiB free), preventing its notification queue from
opening. Both orchestrator replicas were blocked in `wait-for-gitea`; the public
API returned 502 and direct ingress returned 503. The healthy Longhorn volume was
expanded in place to 32Gi, preserving its identity and all repository data. The
HomeLab environment override and incident record were pushed as `3d50cb7`:
`deployments_managed/srw-config/{srw_values_configmap.yaml,README.md}`.
At approximately 18:58 UTC, Gitea and both orchestrator replicas were Ready;
the exact public login URL returned 302 followed by the sign-in page with 200.
Authenticated session completion and VM job execution remain unverified.
Follow-ups: storage alerts, repository growth/retention and removing Gitea as a
hard startup dependency for otherwise usable login/diagnostic endpoints.

| Area | Published | Remaining |
| --- | --- | --- |
| A1 creation retry | Frozen intent, per-effect grants, retained sources/attachment, cancellation discovery/partial disposal, guarded Resume and progress | Final durable cancellation receipts, attachment settlement and terminal transaction (0269 draft); capability and live sentinel/worker acceptance |
| A2/A3 provisioning | Phase clocks, durable capacity waits, bounded controller failure handling, attempt accounting | Live phase/wait/cancel and eventual execution acceptance |
| B recovery | Recovery/fencing/retention code and acceptance tooling | Seven-scenario live matrix: no successful full matrix yet; Longhorn durability and worker continuation; staged enablement |
| C idle lifecycle | Policy, episode storage, selected real Job wait producers and atomic Job/thread exits | Remaining semantic producers/adapters, physical compute release, leases and shared authorized wake |
| D resource limits | Inventory, cost/placement helpers, fairness/waiters, reservation stores and dormant policy lifecycle | Runtime enforcement, full launcher cost, Node/effect binding, physical release accounting, occupancy UX and live capacity acceptance |

Keep creation retry, automatic/replacement recovery, acceptance gate, idle release,
and resource observer/shadow/enforcement disabled as listed in the release note.
The chart count backstop remains `vmController.maxConcurrentVms: 4`; the pilot's
one-job concurrency is an operating choice, not a newly configured resource limit.
No new safe hardware capacity has been established. Original incident job
`d8436836-b7a3-407d-a395-8f456322dfdd` still needs its own verified disposition.

## Preserved local work and evidence

These paths belong to the coordinator machine and will not exist in a new VM:

- `/tmp/srw-vm-reliability-20260919`: original branch
  `feat/vm-reliability-20260919`, committed at `83ac143f0`, with unfinished 0269
  source/tests. Preserve the dirty work; it is not reviewed or ready to publish.
- `/home/ghost/.cache/srw-vm-validation/unfinished-0269-20260920-w7CqV2/`:
  base commit, tracked patch and archive of new source/tests. Transfer a reviewed
  task-specific subset explicitly if a future job needs it; do not expect a Git
  checkout to contain these drafts. Check compatibility with current develop.
- `/home/ghost/.cache/srw-vm-validation/evidence/`: local test and k3d evidence.
  Dedicated cluster `srw-vm-partial-20260919a`, namespace
  `srw-vm-recovery-partial-app-20260919a`, remains preserved. Its frozen
  `a4e3081f8` image predates publication fixes; rebuild before further acceptance.
  Lower-level stop evidence and Ready deployments do not establish recovery.
- `knowledge-base/` is an independent, ignored Git repository with existing
  local edits. Its roadmap is supplementary; application jobs should use this
  tracked handoff and the linked application plans. Do not stage it wholesale.
  Documentation in the original implementation worktree describes earlier local
  checkpoints; this handoff supersedes its publication-status statements.

## Next session: one VM delegation pilot

1. Check current remote develop/CI and main-dev image/schema/flag state. Inspect
   active jobs, existing VMs and eligible-node/storage headroom. Keep the pilot
   limited to one new VM job; do not change recovery flags to make it run.
2. Discover the available expert, VM WorkspaceTemplate and exact configured model
   identifier. Use the owner's economical provider choice where available;
   confirm actual configuration rather than inventing a model ID.
3. Give the job a bounded checkout-and-test task first. It should read this handoff,
   report its commit/environment and run the three fixture regression modules:
   `tests/test_job_create_wire.py`, `tests/test_manifest_runtime_ownership.py`,
   `tests/test_queue_job_for_resume.py`. Use `PYTHONPATH=src`, the repo's dependency
   setup and a small worker count (for example two); record passes and skips.
4. Verify VM readiness, actual worker claim/command execution, disk headroom,
   repository credentials, Python tooling and a working container runtime for
   real PostgreSQL tests. Capture the job ID and logs. VM Running alone is not
   success, and skipped database tests do not prove the VM can run them.
5. After the pilot succeeds, schedule one small implementation slice with explicit
   acceptance tests, a feature branch and a reviewable PR. Preserve all existing
   runtime/disk authority. Avoid asking one job to finish all A1/B/C/D work.

Most code, unit tests and containerized PostgreSQL tests are suitable for the VM
once the pilot proves its tools. Nested k3d/KubeVirt requires additional runtime
and virtualization capabilities; test these before assigning it. Keep that
integration work on the coordinator machine if unavailable, with an explicit
handoff. Do not treat ordinary VM job success as the recovery acceptance gate.
