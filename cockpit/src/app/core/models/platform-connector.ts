/**
 * Platform-owned connectors: the ones SRW provisions and manages itself.
 *
 * Such a row carries `managed_key`, `<kind>:<uuid>`; the uuid names the
 * project or the user the connector belongs to. Users attach these like any
 * other connector, but nobody edits, deletes, links or unlinks one: the
 * server refuses, and the cockpit shows them as managed. The cockpit's copy
 * of `shared/connectors/platform.py`, including its one-release fallback: a
 * `kb` row written before the key carries only `config.native_project_id`.
 *
 * Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
 * (native rows, "What people see").
 */
import type {Datasource} from './api.model';

/** A project's own knowledge base. */
export const PROJECT_KB = 'project-kb';
/** A project's folder in the main cloud, mounted in its sessions. */
export const PROJECT_CLOUD = 'project-cloud';
/** A project's folder copied into a job's workspace. */
export const PROJECT_CLOUD_CHECKOUT = 'project-cloud-checkout';
/** A user's personal root in the main cloud. */
export const USER_CLOUD_ROOT = 'user-cloud-root';
/** A user's session outbox in the main cloud. */
export const CLOUD_OUTBOX = 'cloud-outbox';

export type PlatformConnectorKind =
  | typeof PROJECT_KB
  | typeof PROJECT_CLOUD
  | typeof PROJECT_CLOUD_CHECKOUT
  | typeof USER_CLOUD_ROOT
  | typeof CLOUD_OUTBOX;

/** Kinds whose uuid is a project: that project's own connectors. */
const PROJECT_KINDS: ReadonlySet<string> = new Set([
  PROJECT_KB,
  PROJECT_CLOUD,
  PROJECT_CLOUD_CHECKOUT,
]);
/** Kinds whose uuid is a user: that user's own connectors. */
const USER_KINDS: ReadonlySet<string> = new Set([USER_CLOUD_ROOT, CLOUD_OUTBOX]);

/** The row fields the predicates read. */
export type PlatformRow =
  | Partial<Pick<Datasource, 'type' | 'config' | 'managed_key'>>
  | null
  | undefined;

/** Why SRW owns this connector, as its managed key, or null for a user's
 *  own. A `kb` row without a key still counts when it carries the older
 *  native marker, as the server counts it. */
export function platformOwnedKey(ds: PlatformRow): string | null {
  if (!ds) return null;
  if (typeof ds.managed_key === 'string' && ds.managed_key) return ds.managed_key;
  const marker = ds.type === 'kb' ? ds.config?.native_project_id : null;
  return marker ? `${PROJECT_KB}:${marker}` : null;
}

/** Whether SRW provisions and manages this connector. */
export function isPlatformOwned(ds: PlatformRow): boolean {
  return platformOwnedKey(ds) !== null;
}

/** The kind of platform-owned connector this is (a kind this cockpit does
 *  not know passes through as is), or null for a user's own. */
export function platformOwnedKind(ds: PlatformRow): PlatformConnectorKind | string | null {
  const key = platformOwnedKey(ds);
  if (key === null) return null;
  return key.split(':', 1)[0] || null;
}

/** Whose connector a platform-owned row is: a project's, a user's, or (a
 *  kind this cockpit does not know) only SRW's. Null for a user's own. */
export function platformConnectorOwner(ds: PlatformRow): 'project' | 'user' | 'srw' | null {
  const kind = platformOwnedKind(ds);
  if (kind === null) return null;
  if (PROJECT_KINDS.has(kind)) return 'project';
  if (USER_KINDS.has(kind)) return 'user';
  return 'srw';
}

/** The project or user a platform-owned row belongs to (the key's uuid). */
export function platformConnectorOwnerId(ds: PlatformRow): string | null {
  const key = platformOwnedKey(ds);
  if (key === null) return null;
  const at = key.indexOf(':');
  return at < 0 ? null : key.slice(at + 1) || null;
}

const KIND_LABEL_KEYS: ReadonlyMap<string, string> = new Map<PlatformConnectorKind, string>([
  [PROJECT_KB, 'datasources.platform.kind.projectKb'],
  [PROJECT_CLOUD, 'datasources.platform.kind.projectCloud'],
  [PROJECT_CLOUD_CHECKOUT, 'datasources.platform.kind.projectCloudCheckout'],
  [USER_CLOUD_ROOT, 'datasources.platform.kind.userCloudRoot'],
  [CLOUD_OUTBOX, 'datasources.platform.kind.cloudOutbox'],
]);

/** The translation key naming what a platform-owned row is to its user
 *  ("Project folder", "My cloud", …); null for a user's own row and for a
 *  kind this cockpit does not know (callers fall back to the type label). */
export function platformKindLabelKey(ds: PlatformRow): string | null {
  const kind = platformOwnedKind(ds);
  return kind === null ? null : (KIND_LABEL_KEYS.get(kind) ?? null);
}

/** The translation key saying who manages a platform-owned row: its
 *  project, or SRW for its user (for you, when `currentUserId` is that
 *  user). Null for a user's own row. */
export function platformManagedLabelKey(
  ds: PlatformRow,
  currentUserId?: string | null,
): string | null {
  switch (platformConnectorOwner(ds)) {
    case 'project':
      return 'datasources.platform.managedByProject';
    case 'user':
      return currentUserId && platformConnectorOwnerId(ds) === currentUserId
        ? 'datasources.platform.managedForYou'
        : 'datasources.platform.managedForItsUser';
    case 'srw':
      return 'datasources.platform.managedBySrw';
    default:
      return null;
  }
}

/** The connector types the main cloud provides, all platform-owned. */
const CLOUD_CONNECTOR_TYPES: ReadonlySet<string> = new Set([
  'cloud_folder',
  'cloud_folder_checkout',
  'cloud_outbox',
]);

export function isCloudConnectorType(type: string | null | undefined): boolean {
  return CLOUD_CONNECTOR_TYPES.has((type ?? '').toString());
}

const ACCESS_LEVEL_LABEL_KEYS: ReadonlyMap<string, string> = new Map([
  ['read_only', 'datasources.accessLevel.read_only'],
  ['protected', 'datasources.accessLevel.protected'],
  ['read_write', 'datasources.accessLevel.read_write'],
  ['reviewed_write_back', 'datasources.accessLevel.reviewed_write_back'],
]);

/** The translation key of a cloud connector's access level, or null for a
 *  level this cockpit does not name (callers show its id). */
export function accessLevelLabelKey(level: string | null | undefined): string | null {
  return level ? (ACCESS_LEVEL_LABEL_KEYS.get(level) ?? null) : null;
}
