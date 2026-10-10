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
 *
 * Since the main cloud became connectors (main_cloud_as_connectors.md,
 * slices 3 to 6) every attached cloud connector binds on its own: `folders`
 * lists each one with its level and state, and one that was left out says
 * why (`excluded`, keyed by its connector id); nothing takes its place.
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

/** One attached cloud folder, as the session header shows it. */
export interface CloudFolderBinding {
  /** Its name in the workspace, or '' for one that was left out. */
  name: string;
  /** Where the agent finds it, or '' for one that was left out. */
  path: string;
  /** What it is (one of {@link CLOUD_FOLDER_KINDS}), or ''. */
  kind: string;
  /** The level it is bound at: `read_only`, `read_write` or `protected`;
   *  '' when the record does not say. */
  level: string;
  state: 'mounted' | 'pending' | 'unavailable' | 'excluded';
  /** Why it is unavailable or left out (a {@link CLOUD_FOLDER_REASONS}
   *  code), '' otherwise. */
  reason: string;
  /** The connector a left-out folder is (its id), or ''. */
  connectorId: string;
  /** The connector's name, when the live event carries it, or ''. */
  title: string;
}

export interface CloudFolderState {
  problems: CloudFolderProblem[];
  /** Every attached cloud folder, mounted or not, in plan order. */
  folders: CloudFolderBinding[];
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
  // Why an attached cloud connector was left out (the orchestrator's
  // EXCLUSION_REASONS, main cloud slices 3 to 7).
  'access_unavailable',
  'config_invalid',
  'not_eligible',
  'no_session_folder',
  'backend_unavailable',
  'beside_protected',
  'grant_unavailable',
  'reader_unavailable',
  'cloud_not_connected',
  'cloud_grant_revoked',
] as const;

/** What a cloud folder is, as the notice names it: a project's folder, the
 *  owner's own root, the session outbox, a protected session's folder, and
 *  the two kinds of the rule before connectors. */
export const CLOUD_FOLDER_KINDS = [
  'session_folder',
  'project',
  'project_default',
  'user_root',
  'cloud_outbox',
  'protected_lower',
] as const;

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

function excludedEntries(excluded: unknown): Record<string, unknown>[] {
  if (!Array.isArray(excluded)) return [];
  return excluded.filter(
    (entry): entry is Record<string, unknown> => !!entry && typeof entry === 'object',
  );
}

function excludedProblems(excluded: unknown): CloudFolderProblem[] {
  return excludedEntries(excluded).map((entry) => ({
    name: '',
    path: '',
    kind: kindOf(entry['mount_kind']),
    reason: reasonOf(entry['reason']),
  }));
}

function excludedBindings(excluded: unknown): CloudFolderBinding[] {
  return excludedEntries(excluded).map((entry) => ({
    name: '',
    path: '',
    kind: kindOf(entry['mount_kind']),
    level: '',
    state: 'excluded',
    reason: reasonOf(entry['reason']),
    connectorId: typeof entry['source_ref'] === 'string' ? entry['source_ref'] : '',
    title: typeof entry['title'] === 'string' ? entry['title'] : '',
  }));
}

/** The level a planned mount binds at: a protected session's folder is
 *  `protected` (its lower layer mounts read-only). */
function levelOf(kind: unknown, access: unknown): string {
  if (kind === 'protected_lower') return 'protected';
  return access === 'read_only' || access === 'read_write' ? access : '';
}

function stateOf(value: unknown): CloudFolderBinding['state'] {
  return value === 'mounted' || value === 'unavailable' ? value : 'pending';
}

/** From `thread.metadata.cloud_mount_status`. */
export function cloudFolderStateFromStatus(status: unknown): CloudFolderState {
  if (!status || typeof status !== 'object') {
    return {problems: [], folders: [], protected: false, agentOutdated: false};
  }
  const record = status as Record<string, unknown>;
  const mounts = record['mounts'];
  const problems: CloudFolderProblem[] = [];
  const folders: CloudFolderBinding[] = [];
  let protectedDown = false;
  if (mounts && typeof mounts === 'object' && !Array.isArray(mounts)) {
    const entries = Object.entries(mounts as Record<string, unknown>);
    for (const [name, entry] of entries) {
      if (!entry || typeof entry !== 'object') continue;
      const row = entry as Record<string, unknown>;
      const path = pathOf(name, entries.length);
      const state = stateOf(row['state']);
      const reason = state === 'unavailable' ? reasonOf(row['reason']) : '';
      if (state === 'unavailable') {
        problems.push({name, path, kind: '', reason});
        // A protected session's folder that did not come up: it runs
        // without its cloud (decision 42).
        if (row['mount_kind'] === 'protected_lower') protectedDown = true;
      }
      folders.push({
        name,
        path,
        kind: kindOf(row['mount_kind']),
        level: levelOf(row['mount_kind'], row['access']),
        state,
        reason,
        connectorId: '',
        title: '',
      });
    }
  }
  return {
    problems: [...problems, ...excludedProblems(record['excluded'])],
    folders: [...folders, ...excludedBindings(record['excluded'])],
    protected: protectedDown,
    agentOutdated: record['notice'] === 'agent_outdated',
  };
}

/** From a `cloud_mount.status` event's params. The event names each mount
 *  and its state only: what a folder is and its level come from `previous`
 *  (the thread's record), matched by name. */
export function cloudFolderStateFromEvent(
  params: Record<string, unknown>,
  previous?: CloudFolderState | null,
): CloudFolderState {
  const mounts = Array.isArray(params['mounts']) ? params['mounts'] : [];
  const rows = mounts.filter(
    (row): row is Record<string, unknown> => !!row && typeof row === 'object',
  );
  const known = new Map((previous?.folders ?? []).filter((f) => f.name).map((f) => [f.name, f]));
  const problems: CloudFolderProblem[] = [];
  const folders: CloudFolderBinding[] = rows.map((row) => {
    const name = String(row['name'] ?? '');
    const path = pathOf(name, rows.length);
    const state = stateOf(row['state']);
    const reason = state === 'unavailable' ? reasonOf(row['reason']) : '';
    if (state === 'unavailable') problems.push({name, path, kind: '', reason});
    const before = known.get(name);
    return {
      name,
      path,
      kind: before?.kind ?? '',
      level: before?.level ?? '',
      state,
      reason,
      connectorId: '',
      title: '',
    };
  });
  return {
    problems: [...problems, ...excludedProblems(params['excluded'])],
    folders: [...folders, ...excludedBindings(params['excluded'])],
    protected: params['protected'] === true && problems.length > 0,
    // An agent that reports is up to date.
    agentOutdated: false,
  };
}

/** The problems of a `cloud_mount.status` event (kept for its callers). */
export function cloudFolderProblemsFromEvent(params: Record<string, unknown>): CloudFolderProblem[] {
  return cloudFolderStateFromEvent(params).problems;
}
