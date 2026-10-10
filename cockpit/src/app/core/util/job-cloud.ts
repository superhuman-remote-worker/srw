/**
 * A job's main-cloud deliveries, as its record says them (main cloud slice
 * 6): the outbox, a copy of its deliverables SRW makes into a cloud folder
 * shared with the job's owner (`context.job_outbox`, re-synced on demand),
 * and the project folder's copy in its repository (`context.cloud_checkout`),
 * whose changes become a diff to review only at `reviewed_write_back`.
 *
 * Reasons are closed codes; anything unknown reads as the generic one.
 * Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md.
 */
import { asRecord } from './job-status';

/** Statuses whose output is stable enough to copy (the server's
 *  `EXPORTABLE_STATUSES`). */
const EXPORTABLE_STATUSES: ReadonlySet<string> = new Set(['completed', 'pending_review', 'reviewing']);

/** Why a copied folder is not shared with its owner yet. */
export const JOB_OUTBOX_UNSHARED_REASONS = ['no_account', 'rate_limited', 'share_failed'] as const;
/** Why a copy failed. */
export const JOB_OUTBOX_FAILURE_REASONS = [
  'backend_unavailable',
  'forge_unavailable',
  'folder_failed',
  'file_missing',
  'copy_failed',
  'owner_unavailable',
] as const;
/** What the completion capture found in the project folder's copy. */
export const CHECKOUT_CAPTURE_STATES = ['captured', 'no_changes', 'no_baseline', 'head_unreadable'] as const;

const UNSHARED = new Set<string>(JOB_OUTBOX_UNSHARED_REASONS);
const FAILURES = new Set<string>(JOB_OUTBOX_FAILURE_REASONS);
const CAPTURES = new Set<string>(CHECKOUT_CAPTURE_STATES);

/** `context.job_outbox`: what the latest copy did. */
export interface JobOutboxRecord {
  state: 'delivered' | 'failed';
  /** Files copied (delivered only). */
  files: number;
  /** Whether the owner can see the folder (delivered only). */
  shared: boolean;
  /** Why not, when `shared` is false. */
  unshared: (typeof JOB_OUTBOX_UNSHARED_REASONS)[number] | null;
  /** Why the copy failed (failed only). */
  reason: (typeof JOB_OUTBOX_FAILURE_REASONS)[number] | null;
}

export function jobOutboxRecord(context: unknown): JobOutboxRecord | null {
  const record = asRecord(asRecord(context)?.['job_outbox']);
  if (!record) return null;
  if (record['state'] === 'failed') {
    const reason = String(record['reason'] ?? '');
    return {
      state: 'failed',
      files: 0,
      shared: false,
      unshared: null,
      reason: (FAILURES.has(reason) ? reason : 'copy_failed') as JobOutboxRecord['reason'],
    };
  }
  if (record['state'] !== 'delivered') return null;
  const shared = record['shared'] === true;
  const unshared = String(record['unshared'] ?? '');
  return {
    state: 'delivered',
    files: typeof record['files'] === 'number' ? record['files'] : 0,
    shared,
    unshared: shared
      ? null
      : ((UNSHARED.has(unshared) ? unshared : 'share_failed') as JobOutboxRecord['unshared']),
    reason: null,
  };
}

/** What the completion capture found in the project folder's copy
 *  (`context.cloud_checkout.capture.state`), or null before it ran. */
export function checkoutCaptureState(context: unknown): (typeof CHECKOUT_CAPTURE_STATES)[number] | null {
  const checkout = asRecord(asRecord(context)?.['cloud_checkout']);
  const state = String(asRecord(checkout?.['capture'])?.['state'] ?? '');
  return CAPTURES.has(state) ? (state as (typeof CHECKOUT_CAPTURE_STATES)[number]) : null;
}

/** A job whose project-folder copy goes through review, but has no diff to
 *  review: say so rather than show nothing. */
export function noProjectFolderChanges(job: {
  cloud_review_mode?: string | null;
  diff_status?: string | null;
}): boolean {
  return job.cloud_review_mode === 'diff' && !job.diff_status;
}

/**
 * Which outbox affordance a job offers, if any: re-syncing its deliverables
 * into its owner's cloud folder, for any job whose output is stable and
 * whose project-folder diff is not waiting for review (that review comes
 * first).
 *
 * `'export'` and `'open'` are deliberately two separate actions rather than one
 * button that exports-then-opens: the export routinely runs longer than the ~5s
 * of transient user activation a browser grants a click, so opening from its
 * completion callback gets swallowed by the popup blocker. `'exported'` is the
 * degraded state — the export happened but the cloud backend can't currently
 * hand us a URL, so there's nothing to link to.
 */
export type JobCloudAction = 'none' | 'export' | 'open' | 'exported';

export function jobCloudAction(job: {
  status: string;
  diff_status?: string | null;
  exported_at?: string | null;
  exported_folder_url?: string | null;
}): JobCloudAction {
  if (!EXPORTABLE_STATUSES.has(job.status) || job.diff_status === 'pending') return 'none';
  if (!job.exported_at) return 'export';
  return job.exported_folder_url ? 'open' : 'exported';
}
