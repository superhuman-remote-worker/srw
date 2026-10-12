/** Only known, safe owner reasons are mapped to localized copy. */
export function workspaceLifecycleReasonKey(reason: string): string {
  if (reason === 'identity_unverified') return 'jobs.lifecycle.reason.identity';
  if (reason === 'resource_reservation_unavailable' || reason === 'resource_reservation_held') {
    return 'jobs.lifecycle.reason.capacity';
  }
  if (reason === 'active_workspace_access') return 'jobs.lifecycle.reason.access';
  return 'jobs.lifecycle.reason.attention';
}
