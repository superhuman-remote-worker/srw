import { ThreadMount, ThreadProtectedSource } from '../../core/models/api.model';

/**
 * A protected project folder resolved for a thread, safe to link to.
 *
 * Lives in its own module rather than in the review component so the chat
 * service can consume the type and the derivation without importing the
 * component — which would drag `JobDiffReviewComponent` (and Monaco's loader)
 * back into the eager graph and defeat the `@defer` that keeps the review
 * surface out of the initial bundle.
 *
 * See knowledge/features/protected_cloud_review_surface_redesign.md §7.
 */
export interface ProtectedFolderLink {
  /** The project folder's browser URL (`Project.cloud_storage_url`). */
  url: string;
  /** Project name, for the action's accessible label. */
  name: string;
  /** The mount's `target_path`. Must equal the diff summary's
   *  `protected_mount` or the link is not the folder this diff applies to. */
  targetPath: string;
}

/**
 * Which of a thread's mounts is the protected one, mirroring the backend's
 * single definition in `orchestrator/services/cloud_staging/__init__.py`:
 *
 * ```python
 * for row in mount_rows or []:
 *     if (row.get("mount_kind") != "project_default"
 *             and row.get("backend_id") == "nextcloud"
 *             and row.get("cloud_handle")):
 *         return row
 * ```
 *
 * Two deliberate differences:
 *
 * - `cloud_handle` is not in the REST projection, so it cannot be checked
 *   here. That is why the result is only ever *provisional* — the caller must
 *   still confirm the pick against the summary's `protected_mount` before
 *   showing anything (`folderLinkMatches`).
 * - a `source_ref` is required, because without a project id there is nothing
 *   to resolve a browser URL from.
 *
 * Row order is load-bearing: the backend query is `ORDER BY target_path` and
 * the REST projection preserves it, so "first match" means the same row on
 * both sides.
 */
export function selectProtectedProjectMount(
  mounts: readonly ThreadMount[] | null | undefined,
): ThreadMount | null {
  for (const row of mounts ?? []) {
    if (
      row.mount_kind !== 'project_default' &&
      row.backend_id === 'nextcloud' &&
      row.source_ref
    ) {
      return row;
    }
  }
  return null;
}

/**
 * The protected folder of a thread to resolve a link for: its project and
 * where it is mounted.
 *
 * A session protected from its connector (main cloud slice 5) names its
 * folder in `protected_source`, frozen at admission, and that is the folder
 * the review applies to. A session protected before connectors has none, and
 * falls back to {@link selectProtectedProjectMount}. Either answer is still
 * checked against the summary's `protected_mount` (`folderLinkMatches`).
 */
export function protectedFolderSource(
  source: ThreadProtectedSource | null | undefined,
  mounts: readonly ThreadMount[] | null | undefined,
): { projectId: string; targetPath: string } | null {
  if (source?.source_ref && source.target_path) {
    return { projectId: source.source_ref, targetPath: source.target_path };
  }
  const mount = selectProtectedProjectMount(mounts);
  return mount?.source_ref ? { projectId: mount.source_ref, targetPath: mount.target_path } : null;
}

/**
 * Whether a resolved folder link may be shown against a loaded summary.
 *
 * The frontend cannot see `cloud_handle` and so cannot be certain its
 * candidate is the row the backend actually protected. This is the
 * cross-check that makes the link safe: the backend reports the protected
 * mount's `target_path` in every summary, so an exact match proves the two
 * picked the same row. Anything else — no link, no summary, a null
 * `protected_mount`, or a mismatch — means no action is offered rather than a
 * guess. PC-19 was caused by exactly such a guess (the header's Files action
 * preferring a legacy `sessions/<id>` handle over the project mount).
 */
export function folderLinkMatches(
  link: ProtectedFolderLink | null | undefined,
  protectedMount: string | null | undefined,
): boolean {
  if (!link || !protectedMount) return false;
  return link.targetPath === protectedMount;
}
