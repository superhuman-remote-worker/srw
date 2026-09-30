import type {WorkspaceCreationView} from '../models/api.model';

/** Only translate trusted typed combinations; never render backend diagnostic text. */
export function workspaceCreationMessageKey(view: WorkspaceCreationView): string {
  if (view.stage === 'scheduling') {
    if (view.state === 'waiting_capacity' && view.reason_code === 'insufficient_capacity') {
      return 'workspaceCreation.insufficientCapacity';
    }
    if (view.state === 'waiting_capacity' && view.reason_code === 'scheduler_unschedulable') {
      return 'workspaceCreation.waitingScheduling';
    }
    if (view.state === 'observing') {
      if (view.reason_code === 'legacy_receipt_held') return 'workspaceCreation.legacyHeld';
      if (view.reason_code === 'observation_pending' || view.reason_code === 'scheduling_other') {
        return 'workspaceCreation.observing';
      }
    }
  }
  if (view.stage === 'readiness') {
    if (view.state === 'starting' && view.reason_code === 'scheduled') {
      return 'workspaceCreation.starting';
    }
    if (view.state === 'attention') {
      switch (view.reason_code) {
        case 'invalid_image': return 'workspaceCreation.invalidImage';
        case 'invalid_configuration': return 'workspaceCreation.invalidConfiguration';
        case 'pull_deadline': return 'workspaceCreation.pullDeadline';
        case 'readiness_deadline': return 'workspaceCreation.readinessDeadline';
        case 'ssh_deadline': return 'workspaceCreation.sshDeadline';
      }
    }
  }
  return 'workspaceCreation.unknown';
}
