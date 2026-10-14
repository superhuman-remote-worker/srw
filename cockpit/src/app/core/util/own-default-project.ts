import type {ProjectMemberRole} from '../models/api.model';

/**
 * The caller's own default (personal) project, never another user's.
 *
 * `is_default` alone is not enough: an admin's fleet-wide list holds every
 * user's default project, and even a self-scoped list can include a default
 * project the caller is only a member of. Only the owner's one is theirs; a
 * server create against any other answers 403.
 *
 * Even then two such rows can appear (a colleague can make the caller a
 * co-owner of their personal project), so when the caller's own
 * `default_project_id` (from `/auth/me`) is known and in the list, it wins.
 */
export function pickOwnDefaultProject<T extends {id: string; is_default?: boolean; user_role?: ProjectMemberRole | null}>(
  projects: readonly T[],
  ownDefaultProjectId?: string | null,
): T | undefined {
  if (ownDefaultProjectId) {
    const own = projects.find((p) => p.id === ownDefaultProjectId);
    if (own) return own;
  }
  return projects.find((p) => p.is_default === true && p.user_role === 'owner');
}
