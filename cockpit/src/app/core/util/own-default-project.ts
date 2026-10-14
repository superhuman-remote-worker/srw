import type {ProjectMemberRole} from '../models/api.model';

/**
 * The caller's own default (personal) project, never another user's.
 *
 * `is_default` alone is not enough: an admin's fleet-wide list holds every
 * user's default project, and even a self-scoped list can include a default
 * project the caller is only a member of. Only the owner's one is theirs; a
 * server create against any other answers 403.
 */
export function pickOwnDefaultProject<T extends {is_default?: boolean; user_role?: ProjectMemberRole | null}>(
  projects: readonly T[],
): T | undefined {
  return projects.find((p) => p.is_default === true && p.user_role === 'owner');
}
