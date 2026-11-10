import {TranslocoService} from '@jsverse/transloco';
import {WorkspaceContractProjection} from '../models/api.model';
import {effectiveJobStatus, isTerminalJobStatus} from './job-status';

// A job that stopped and waits on nobody but a person: no runtime is expected.
const STOPPED_STATUSES: ReadonlySet<string> = new Set(['pending_review']);

interface WorkspaceTitleJob {
  status?: string | null;
  completion_outcome_kind?: string | null;
  workspace_recovery?: {state?: string | null} | null;
  workspace_contract?: WorkspaceContractProjection | null;
  vm_creation?: {state?: string | null; message?: string | null; resumable?: boolean} | null;
}

/**
 * A stopped job with no runtime attached. Its sandbox or VM goes away once the
 * job stops, so this is the normal end state, not a fault: the summary leaves
 * the effective segment out instead of printing "unavailable", and the line is
 * not styled as a warning. It does not claim the cleanup settled (the title
 * keeps describing that). A VM job with a creation record keeps the old line.
 */
export function workspaceRuntimeAtRest(job: WorkspaceTitleJob | null | undefined): boolean {
  const workspace = job?.workspace_contract;
  if (!workspace || job?.workspace_recovery || job?.vm_creation) return false;
  if (workspace.effective_backend != null || workspace.state !== 'waiting') return false;
  const status = effectiveJobStatus(job);
  return isTerminalJobStatus(status) || STOPPED_STATUSES.has(status);
}

export function workspaceContractSummary(job: WorkspaceTitleJob | null | undefined, transloco: TranslocoService): string {
  const workspace = job?.workspace_contract;
  if (!workspace) return '';
  const requested = workspace.requested_backend ?? transloco.translate('jobs.workspace.default');
  const assigned = workspace.assigned_backend ?? transloco.translate('jobs.workspace.unavailable');
  if (workspaceRuntimeAtRest(job)) {
    return transloco.translate('jobs.workspace.summaryStopped', {requested, assigned});
  }
  return transloco.translate('jobs.workspace.summary', {
    requested,
    assigned,
    effective: workspace.effective_backend ?? transloco.translate('jobs.workspace.unavailable'),
  });
}

/** Warning style for a workspace that is not ready, except a stopped job at rest. */
export function workspaceContractWarns(job: WorkspaceTitleJob | null | undefined): boolean {
  const workspace = job?.workspace_contract;
  return !!workspace && workspace.state !== 'ready' && !workspaceRuntimeAtRest(job);
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
