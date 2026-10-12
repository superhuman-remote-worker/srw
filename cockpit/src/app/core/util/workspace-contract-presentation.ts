import {TranslocoService} from '@jsverse/transloco';
import {WorkspaceContractProjection} from '../models/api.model';
import {effectiveJobStatus, isTerminalJobStatus} from './job-status';

interface WorkspaceTitleJob {
  status?: string | null;
  completion_outcome_kind?: string | null;
  workspace_recovery?: {state?: string | null} | null;
  workspace_contract?: WorkspaceContractProjection | null;
  vm_creation?: {state?: string | null; message?: string | null; resumable?: boolean} | null;
}

/** Describe runtime availability without inferring whether terminal cleanup settled. */
export function workspaceContractTitle(job: WorkspaceTitleJob | null | undefined, transloco: TranslocoService): string {
  const workspace = job?.workspace_contract;
  if (!workspace) return '';

  if (
    isTerminalJobStatus(effectiveJobStatus(job)) &&
    workspace.assigned_backend === 'vm' &&
    workspace.state === 'waiting' &&
    workspace.failure === 'vm_runtime_not_ready'
  ) {
    if (job?.vm_creation?.state === 'cancel_requested' && job.vm_creation.message) {
      return transloco.translate('jobs.workspace.terminalUnavailableWithPending', {
        message: job.vm_creation.message,
      });
    }
    return transloco.translate('jobs.workspace.terminalUnavailable');
  }

  return transloco.translate('jobs.workspace.state', {
    state: workspace.state,
    failure: workspace.failure ?? transloco.translate('jobs.workspace.none'),
  });
}
