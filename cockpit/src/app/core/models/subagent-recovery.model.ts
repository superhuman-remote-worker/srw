/**
 * The marker a session delegation-batch recovery stamps on the rows it adds to
 * a parent transcript: `thread_messages.metrics.subagent_recovery`, which the
 * history endpoint returns with every row.
 *
 * When the executor running a session dies during a turn that delegated work,
 * its successor settles the turn once: one `role=tool` row for every
 * `delegate_agent` call without a result yet, then one `role=event`
 * continuation (`source=subagent`) that supersedes the abandoned input. The
 * text of those rows is written for the model; this marker is what the cockpit
 * reads instead, so nothing here ever parses the text.
 *
 * A row without the marker — everything else, including a single-call turn
 * recovered by the older per-child path — parses to null and renders as it
 * always has. So does a marker of a version or class this build does not know.
 *
 * Contract: `src/shared/session_subagent_batch.py` (`result_metrics`,
 * `continuation_metrics`). Design:
 * knowledge-base/knowledge/features/parallel_subagents.md §5.4, §6.5 step 3.
 */

export const SUBAGENT_RECOVERY_METRICS_KEY = 'subagent_recovery';
export const SUBAGENT_RECOVERY_VERSION = 1;

/**
 * What the settle found for one call. `completed`: the child finished but its
 * report never reached the parent; `interrupted`: the child was still queued
 * or running; `not_started`: no child was ever recorded; `declined`: the user
 * declined the permission request; `retired`: the child was cancelled when the
 * session was stopped.
 */
export type SubagentRecoveryClass = 'completed' | 'interrupted' | 'not_started' | 'declined' | 'retired';

const RECOVERY_CLASSES: ReadonlySet<string> = new Set<SubagentRecoveryClass>([
    'completed',
    'interrupted',
    'not_started',
    'declined',
    'retired',
]);

/** The marker of one recovered tool result. */
export interface SubagentRecoveryResult {
    readonly class: SubagentRecoveryClass;
    /** The child's lifecycle status the settle recorded; null without a child. */
    readonly subagentStatus: string | null;
}

/** The marker of the continuation: call counts for the whole turn. */
export interface SubagentRecoveryContinuation {
    /** Every `delegate_agent` call of the turn, including ones already answered. */
    readonly calls: number;
    readonly finished: number;
    readonly interrupted: number;
    readonly notStarted: number;
    readonly declined: number;
    readonly retired: number;
}

function marker(metrics: unknown, kind: 'result' | 'continuation'): Record<string, unknown> | null {
    if (!metrics || typeof metrics !== 'object') return null;
    const raw = (metrics as Record<string, unknown>)[SUBAGENT_RECOVERY_METRICS_KEY];
    if (!raw || typeof raw !== 'object') return null;
    const m = raw as Record<string, unknown>;
    return m['version'] === SUBAGENT_RECOVERY_VERSION && m['kind'] === kind ? m : null;
}

/**
 * The marker of a recovered tool result, or null. `toolCallId` is the row's
 * own `tool_call_id`: a marker naming another call is not trusted.
 */
export function parseSubagentRecoveryResult(
    metrics: unknown,
    toolCallId: string,
): SubagentRecoveryResult | null {
    const m = marker(metrics, 'result');
    if (!m) return null;
    const cls = m['class'];
    if (typeof cls !== 'string' || !RECOVERY_CLASSES.has(cls)) return null;
    if (m['tool_call_id'] !== toolCallId) return null;
    const status = m['subagent_status'];
    return {
        class: cls as SubagentRecoveryClass,
        subagentStatus: typeof status === 'string' && status ? status : null,
    };
}

/** The marker of a recovery continuation, or null. */
export function parseSubagentRecoveryContinuation(metrics: unknown): SubagentRecoveryContinuation | null {
    const m = marker(metrics, 'continuation');
    if (!m) return null;
    const count = (key: string): number | null => {
        const value = m[key];
        return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : null;
    };
    const calls = count('calls');
    const finished = count('finished');
    const interrupted = count('interrupted');
    const notStarted = count('not_started');
    const declined = count('declined');
    const retired = count('retired');
    if (
        calls === null ||
        finished === null ||
        interrupted === null ||
        notStarted === null ||
        declined === null ||
        retired === null
    ) {
        return null;
    }
    return {calls, finished, interrupted, notStarted, declined, retired};
}
