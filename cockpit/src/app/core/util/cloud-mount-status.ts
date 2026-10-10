/**
 * What a session's cloud folders are doing (connector drivers D7).
 *
 * A session workspace whose folders come from the in-pod plane starts even
 * when a folder does not mount; the folder is marked unavailable and the
 * reason reaches the user. The orchestrator keeps the state in
 * `thread.metadata.cloud_mount_status`, and the agent pushes changes as the
 * `cloud_mount.status` event. Reasons are closed codes, never a remote's
 * words; anything unknown reads as `mount_failed`.
 */

export interface CloudFolderProblem {
  /** The folder under workspace/cloud, or '' for one that was not attached. */
  name: string;
  reason: string;
}

export interface CloudFolderState {
  problems: CloudFolderProblem[];
  /** The session's agent predates the plane: folders are under /cloud, unmanaged. */
  agentOutdated: boolean;
}

export const CLOUD_FOLDER_REASONS = [
  'credential_rejected',
  'not_found',
  'unreachable',
  'timeout',
  'mount_failed',
  'config_missing',
  'sidecar_unavailable',
  'unbuildable',
  'set_fallback',
  'too_many_mounts',
  'protected_unavailable',
] as const;

const KNOWN = new Set<string>(CLOUD_FOLDER_REASONS);

function reasonOf(value: unknown): string {
  return typeof value === 'string' && KNOWN.has(value) ? value : 'mount_failed';
}

function excludedProblems(excluded: unknown): CloudFolderProblem[] {
  if (!Array.isArray(excluded)) return [];
  return excluded
    .filter((entry): entry is Record<string, unknown> => !!entry && typeof entry === 'object')
    .map((entry) => ({name: '', reason: reasonOf(entry['reason'])}));
}

/** From `thread.metadata.cloud_mount_status`. */
export function cloudFolderStateFromStatus(status: unknown): CloudFolderState {
  if (!status || typeof status !== 'object') return {problems: [], agentOutdated: false};
  const record = status as Record<string, unknown>;
  const mounts = record['mounts'];
  const problems: CloudFolderProblem[] = [];
  if (mounts && typeof mounts === 'object' && !Array.isArray(mounts)) {
    for (const [name, entry] of Object.entries(mounts as Record<string, unknown>)) {
      if (entry && typeof entry === 'object' && (entry as Record<string, unknown>)['state'] === 'unavailable') {
        problems.push({name, reason: reasonOf((entry as Record<string, unknown>)['reason'])});
      }
    }
  }
  return {
    problems: [...problems, ...excludedProblems(record['excluded'])],
    agentOutdated: record['notice'] === 'agent_outdated',
  };
}

/** From a `cloud_mount.status` event's params. */
export function cloudFolderProblemsFromEvent(params: Record<string, unknown>): CloudFolderProblem[] {
  const mounts = Array.isArray(params['mounts']) ? params['mounts'] : [];
  const problems = mounts
    .filter(
      (row): row is Record<string, unknown> =>
        !!row && typeof row === 'object' && (row as Record<string, unknown>)['state'] === 'unavailable',
    )
    .map((row) => ({name: String(row['name'] ?? ''), reason: reasonOf(row['reason'])}));
  return [...problems, ...excludedProblems(params['excluded'])];
}
