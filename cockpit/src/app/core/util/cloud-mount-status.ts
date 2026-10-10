/**
 * What a session's cloud folders are doing (connector drivers D7).
 *
 * A session workspace whose folders come from the in-pod plane starts even
 * when a folder does not mount; the folder is marked unavailable and the
 * reason reaches the user. The orchestrator keeps the state in
 * `thread.metadata.cloud_mount_status`, and the agent pushes changes as the
 * `cloud_mount.status` event. Reasons are closed codes, never a remote's
 * words; anything unknown reads as `mount_failed`.
 *
 * A protected session whose cloud layer did not come up runs with no cloud
 * folder at all (decision 42): `protected` says so, and the notice reads
 * "protected cloud unavailable" with the reason.
 */

export interface CloudFolderProblem {
  /** The folder's name, or '' for one that was not attached. */
  name: string;
  /** Where the agent finds it: workspace/cloud for a session's only folder,
   * workspace/cloud/<name> for one of several, '' when not attached. */
  path: string;
  /** For a folder that was not attached: what it was (session_folder,
   * project…), or ''. */
  kind: string;
  reason: string;
}

export interface CloudFolderState {
  problems: CloudFolderProblem[];
  /** A protected session runs without its cloud. */
  protected: boolean;
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

/** What a folder that was not attached was, as the notice names it. */
export const CLOUD_FOLDER_KINDS = ['session_folder', 'project', 'project_default'] as const;

const KNOWN = new Set<string>(CLOUD_FOLDER_REASONS);
const KINDS = new Set<string>(CLOUD_FOLDER_KINDS);

function reasonOf(value: unknown): string {
  return typeof value === 'string' && KNOWN.has(value) ? value : 'mount_failed';
}

function kindOf(value: unknown): string {
  return typeof value === 'string' && KINDS.has(value) ? value : '';
}

function pathOf(name: string, total: number): string {
  return total === 1 ? 'workspace/cloud' : `workspace/cloud/${name}`;
}

function excludedProblems(excluded: unknown): CloudFolderProblem[] {
  if (!Array.isArray(excluded)) return [];
  return excluded
    .filter((entry): entry is Record<string, unknown> => !!entry && typeof entry === 'object')
    .map((entry) => ({
      name: '',
      path: '',
      kind: kindOf(entry['mount_kind']),
      reason: reasonOf(entry['reason']),
    }));
}

/** From `thread.metadata.cloud_mount_status`. */
export function cloudFolderStateFromStatus(status: unknown): CloudFolderState {
  if (!status || typeof status !== 'object') {
    return {problems: [], protected: false, agentOutdated: false};
  }
  const record = status as Record<string, unknown>;
  const mounts = record['mounts'];
  const problems: CloudFolderProblem[] = [];
  let isProtected = false;
  if (mounts && typeof mounts === 'object' && !Array.isArray(mounts)) {
    const entries = Object.entries(mounts as Record<string, unknown>);
    for (const [name, entry] of entries) {
      if (!entry || typeof entry !== 'object') continue;
      const row = entry as Record<string, unknown>;
      if (row['mount_kind'] === 'protected_lower') isProtected = true;
      if (row['state'] === 'unavailable') {
        problems.push({name, path: pathOf(name, entries.length), kind: '', reason: reasonOf(row['reason'])});
      }
    }
  }
  return {
    problems: [...problems, ...excludedProblems(record['excluded'])],
    protected: isProtected && problems.length > 0,
    agentOutdated: record['notice'] === 'agent_outdated',
  };
}

/** From a `cloud_mount.status` event's params. */
export function cloudFolderStateFromEvent(params: Record<string, unknown>): CloudFolderState {
  const mounts = Array.isArray(params['mounts']) ? params['mounts'] : [];
  const rows = mounts.filter(
    (row): row is Record<string, unknown> => !!row && typeof row === 'object',
  );
  const problems = rows
    .filter((row) => row['state'] === 'unavailable')
    .map((row) => {
      const name = String(row['name'] ?? '');
      return {name, path: pathOf(name, rows.length), kind: '', reason: reasonOf(row['reason'])};
    });
  return {
    problems: [...problems, ...excludedProblems(params['excluded'])],
    protected: params['protected'] === true && problems.length > 0,
    // An agent that reports is up to date.
    agentOutdated: false,
  };
}

/** The problems of a `cloud_mount.status` event (kept for its callers). */
export function cloudFolderProblemsFromEvent(params: Record<string, unknown>): CloudFolderProblem[] {
  return cloudFolderStateFromEvent(params).problems;
}
