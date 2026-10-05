---
guide_id: sessions.protected-cloud
content_type: how_to
capability_ids:
  - sessions.protected-cloud
journey_ids:
  - sessions.protected-cloud.create
  - sessions.protected-cloud.review
---

# Protected Cloud — stage session changes before they reach the cloud

An ordinary writable cloud mount is **live**: an agent's write or delete can
reach the real folder immediately. **Protected Cloud** gives one persistent
session a read-only view of a project folder plus a private staging layer.
Files under `workspace/cloud` look writable to the agent, but additions,
edits, and deletions remain staged until the user explicitly applies the
whole diff.

This is different from a project's **read-only** cloud setting. Read-only
prevents writes; Protected Cloud lets the agent prepare writes for review.
It is also different from a project job's pending-review diff, which uses the
job workflow and retains git history.

## In this guide

- [When the checkbox is available](#when-the-checkbox-is-available)
- [Create and use a protected session](#create-and-use-a-protected-session)
- [Safety and failure behavior](#safety-and-failure-behavior)
- [If it seems unavailable](#if-it-seems-unavailable)

## When the checkbox is available

Protected Cloud is deployment-dependent and currently requires all of:

- the deployment's Protected Cloud feature flag is enabled;
- at least one selected project is a **non-default Nextcloud** project with a
  cloud folder; and
- the new session uses the **Container** workspace.

It is not the current path for the personal/default project, OpenCloud,
Google Drive, Microsoft 365, Virtual, None, or VM workspaces. If several
project mounts are attached, v1 protects the **first eligible Nextcloud
project mount**; the Cloud changes tooltip names the protected mount.

The checkbox is a creation-time choice and cannot be switched during a
session. The static guide cannot tell whether this deployment enabled it:
the presence of the checkbox and the session's actual cloud state are the
authority.

## Create and use a protected session

1. Go to **Sessions → New Session** and select the eligible project.
2. Enable **Protected cloud — agent writes are staged for your review**.
3. In the **Workspace** field, choose a **Container** template (for example
   `container-full`), then create the session.
4. Ask the agent to work in `workspace/cloud`. It should describe completed
   cloud writes as “staged for your review,” not saved or shared.
5. After a turn stages at least one change, click **Cloud changes (N)** in the
   session status bar. Inspect each added, modified, or deleted path.
6. Choose **Apply to cloud** to write every staged change, or **Reject all** to
   discard every staged change.

Review is file-by-file, but the decision is whole-diff in v1: there is no
per-file apply/reject. Text files get an old/new diff. Binary files are
applied byte-for-byte but show a no-preview placeholder.

The agent cannot apply or reject the diff for the user. Those owner-only
actions require an explicit Cockpit confirmation. Reject is permanent—there
is no protected-session git audit trail—and apply is not reversible from
Cockpit, so use the cloud provider's own history or trash if recovery matters.

## Safety and failure behavior

- Protected Cloud is **fail-closed**. If read-only engagement fails, the
  session gets no project cloud mount; it never falls back to a live writable
  mount. Start a new Container session or ask an administrator to inspect the
  recorded protected-cloud error.
- Staging runs after turns and is best-effort. A failed stage does not fail the
  chat turn; the previous staged epoch remains. The badge tooltip's
  **last staged** time helps identify stale review data.
- Apply is pinned to the exact staged epoch the panel loaded. If a newer turn
  staged changes meanwhile, Cockpit reloads the latest diff instead of applying
  stale content.
- If someone changed a touched cloud path externally, apply is blocked rather
  than overwriting it. Reconcile that cloud file manually, then review again,
  or reject the staged set.
- A partial apply can leave some writes already in the cloud while retaining
  the staged set and errors. Fix the cause and retry; the write operations are
  designed to be safe to repeat.
- If the private staging area reaches its quota, reads still work but new
  writes are blocked. Apply or reject the pending diff to free it.

Staged data is stored separately from the live cloud so review and apply can
survive a workspace pod going away. A current v1 edge case remains: applying
or rejecting while the pod is dead can let an old snapshot re-stage
already-resolved, identical content after resume; reject that duplicate diff.

## If it seems unavailable

- **No checkbox:** verify that a non-default Nextcloud project is selected;
  otherwise the deployment flag may be off.
- **Checkbox selected but no `workspace/cloud`:** Protected Cloud requires a
  Container, or the read-only engage failed. It will not silently mount live
  storage.
- **No Cloud changes badge:** the badge appears only when the staged count is
  greater than zero. Finish a turn that changes `workspace/cloud`; if repeated
  turns never stage, ask an administrator to check the staging transport and
  service health.
- **Apply says the diff changed:** review the automatically reloaded epoch.
- **Apply reports external modifications or partial writes:** do not look for
  a force button; resolve the listed paths or service failure first.
