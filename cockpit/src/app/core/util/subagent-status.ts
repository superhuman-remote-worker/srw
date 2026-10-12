import {BadgeTone} from '../../ui/badge/badge.component';
import {JobSubagentStatus} from '../models/api.model';

/**
 * Shared vocabulary for an in-process child's lifecycle (`subagent_status`).
 *
 * The job detail panel's roster was its only consumer; the session's subagent
 * fan-out card is the second, so the tone moved here rather than being copied.
 * `job-detail-panel.component.ts` re-exports it under the old name.
 */

/** Every lifecycle status the roster publishes today. */
export const SUBAGENT_STATUSES: ReadonlySet<JobSubagentStatus> = new Set<JobSubagentStatus>([
    'queued', 'running', 'completed', 'parked', 'interrupted', 'capped', 'error', 'cancelled',
]);

/** Badge tone for a child's lifecycle status. */
export function subagentStatusTone(status: JobSubagentStatus): BadgeTone {
    switch (status) {
        case 'completed': return 'success';
        case 'running': return 'accent';
        case 'queued': return 'info';
        case 'error':
        case 'cancelled': return 'danger';
        case 'parked':
        case 'interrupted':
        case 'capped': return 'warning';
    }
}

/** A child that can still change on its own — worth polling for. */
export function isLiveSubagentStatus(status: string | null | undefined): boolean {
    return status === 'queued' || status === 'running';
}
