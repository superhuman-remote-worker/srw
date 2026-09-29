import {BadgeTone} from '../../ui/badge/badge.component';
import {isLiveSubagentStatus, SUBAGENT_STATUSES, subagentStatusTone} from '../util/subagent-status';
import {JobSubagent, JobSubagentStatus} from './api.model';
import {ToolCardStatus, ToolCardView} from './tool-card.model';
import {ToolCallEvent} from './turn.model';

/**
 * View model for a subagent fan-out card (`delegate_batch`): pure functions,
 * no Angular, so the rules live where a spec can reach them.
 *
 * A row joins two sources. The `delegate_agent` call is what the transcript
 * knows: type, brief, and the call's own status. The child's `threads` row is
 * what the session roster knows (`GET /api/persistent/threads/{id}/subagents`):
 * handle, lifecycle status, turns, tokens, transcript id. The join key is the
 * row's `parent_tool_call_id`. Live child state stays out of the members for
 * the reason `ToolCardEntity` gives: members are memoized per group object.
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §6.5.
 */

/**
 * How long after a call started a roster read without its child row means the
 * child is queued rather than still being created. Queued children wait for a
 * whole sibling run, so a few seconds of "running" first costs nothing.
 */
export const QUEUED_AFTER_MS = 2_000;

/** One session's children as last read by `SubagentWatchService`. */
export interface SubagentRosterSnapshot {
    /** Child rows keyed by `parent_tool_call_id`. Empty = no child started yet. */
    readonly byCall: ReadonlyMap<string, JobSubagent>;
    /** Client clock when the request behind this read was sent. */
    readonly requestedAt: number;
}

/** One `delegate_agent` call of a fan-out, before any live child state. */
export interface DelegateBatchMember {
    /** The tool-call id; the child row's `parent_tool_call_id`. */
    readonly callId: string;
    readonly subagentType: string;
    readonly description: string;
    /** The call's status as its own tool card shows it. */
    readonly callStatus: ToolCardStatus;
    /**
     * A result reached the transcript. False while the call runs, and also
     * for a call REST history shows without its result: the parent AI row is
     * persisted before any child starts, so a reload mid-fan-out yields calls
     * whose status reads `completed` with nothing behind them.
     */
    readonly answered: boolean;
    /** When the call was first seen (client clock; history: the AI row's time). */
    readonly startedAt: number;
    /** Render-ready card for the row's expandable body. */
    readonly view: ToolCardView;
}

/**
 * What a row's status pill says. The child lifecycle (`JobSubagentStatus`)
 * plus the states in which no child exists: the call is waiting on a
 * permission decision (`pending`), was refused (`denied`), or its gate went
 * unanswered (`expired`).
 *
 * `not_started` is reserved for step 3 of §6.5: a call a recovery settled
 * before it ever ran. Nothing produces it yet — see {@link delegateMemberStatus}.
 */
export type DelegateMemberStatus =
    | JobSubagentStatus
    | 'pending'
    | 'denied'
    | 'expired'
    | 'not_started';

export function buildDelegateBatchMembers(
    events: readonly ToolCallEvent[],
    toView: (event: ToolCallEvent) => ToolCardView,
): DelegateBatchMember[] {
    return events.map((event) => {
        const view = toView(event);
        return {
            callId: event.id,
            subagentType: stringArg(event.args, 'subagent_type'),
            description: stringArg(event.args, 'description'),
            callStatus: view.status,
            answered: event.result !== undefined,
            startedAt: event.startedAt,
            view,
        };
    });
}

function stringArg(args: Record<string, unknown> | undefined, key: string): string {
    const value = args?.[key];
    return typeof value === 'string' ? value.trim() : '';
}

/**
 * The status a row shows. The one place that decides it.
 *
 * - A call that never reached a child says so: `pending`, `denied`, `expired`.
 * - For a call without a result — running live, or shown by history before
 *   its result was written — the child row, once read, is the truth, live or
 *   finished. The call's own status cannot tell a running child from one
 *   queued behind the cap, it stays `running` until the slowest sibling
 *   finishes because the session reports every result of a batch at once
 *   (plan §4, F5), and history marks an unanswered call `completed`.
 * - Such a call with no child row is `queued`: the runtime opens the child row
 *   only when the child takes a slot (plan §4, F4). Only a read sent at least
 *   {@link QUEUED_AFTER_MS} after the call started counts — the first poll of a
 *   fresh batch races the row insert of a child that is starting.
 * - Unless `superseded`: a later assistant answer follows the call's message,
 *   so the turn moved on without this result. That is a crash state a recovery
 *   settles (WP1/WP2), not a queue; with no child row the call keeps its own
 *   state until step 3 below says what happened.
 * - Once the call has returned, a terminal child row still wins (`capped`,
 *   `interrupted` say more than "completed"), but a stale non-terminal poll
 *   does not outvote the result.
 *
 * TODO(parallel_subagents §6.5 step 3): recovered batches. WP1's settle writes
 * a tool result for every call it closes, and marks an interrupted child and a
 * call that never started in a structured field on that tool result row. Carry
 * that field from the history row (and the live frame, if it gets one) onto
 * `ToolCallEvent` and `DelegateBatchMember`, and map it here — before the child
 * row, since the settle is the later fact — to `interrupted` and `not_started`.
 * Do not parse the result text.
 */
export function delegateMemberStatus(
    member: Pick<DelegateBatchMember, 'callStatus' | 'answered' | 'startedAt'>,
    child: JobSubagent | null,
    rosterRequestedAt: number | null,
    superseded = false,
): DelegateMemberStatus {
    // A status this build does not know (a newer server) is treated as no
    // row, so the pill falls back to the call's own state, never a raw key.
    const childStatus = child && SUBAGENT_STATUSES.has(child.status) ? child.status : null;
    const ownStatus = (): DelegateMemberStatus =>
        member.callStatus === 'running' ? 'running'
            : member.callStatus === 'error' ? 'error'
                : 'completed';
    switch (member.callStatus) {
        case 'pending':
        case 'denied':
        case 'expired':
            return member.callStatus;
    }
    if (!member.answered) {
        if (childStatus) return childStatus;
        if (superseded) return ownStatus();
        return !child &&
            rosterRequestedAt !== null &&
            rosterRequestedAt - member.startedAt >= QUEUED_AFTER_MS
            ? 'queued'
            : 'running';
    }
    if (childStatus && !isLiveSubagentStatus(childStatus)) return childStatus;
    return ownStatus();
}

/**
 * Translation key per status. Child states reuse the job roster's vocabulary;
 * only the states without a child have their own keys.
 */
export const DELEGATE_STATUS_LABEL_KEYS: Readonly<Record<DelegateMemberStatus, string>> = {
    queued: 'jobs.detail.subagentsStatuses.queued',
    running: 'jobs.detail.subagentsStatuses.running',
    completed: 'jobs.detail.subagentsStatuses.completed',
    parked: 'jobs.detail.subagentsStatuses.parked',
    interrupted: 'jobs.detail.subagentsStatuses.interrupted',
    capped: 'jobs.detail.subagentsStatuses.capped',
    error: 'jobs.detail.subagentsStatuses.error',
    cancelled: 'jobs.detail.subagentsStatuses.cancelled',
    pending: 'toolCard.delegateBatch.status.pending',
    denied: 'toolCard.delegateBatch.status.denied',
    expired: 'toolCard.delegateBatch.status.expired',
    not_started: 'toolCard.delegateBatch.status.notStarted',
};

export function delegateMemberTone(status: DelegateMemberStatus): BadgeTone {
    switch (status) {
        case 'pending':
            return 'warning';
        case 'denied':
            return 'danger';
        case 'expired':
        case 'not_started':
            return 'neutral';
        default:
            return subagentStatusTone(status);
    }
}

/** Not finished: waiting on a decision, queued, or running. */
export function isLiveDelegateStatus(status: DelegateMemberStatus): boolean {
    return status === 'pending' || isLiveSubagentStatus(status);
}

/**
 * Whether this row still needs the roster polled. A call waiting on a
 * permission decision has no child to watch; it starts polling when approved.
 */
export function needsChildPolling(status: DelegateMemberStatus): boolean {
    return isLiveSubagentStatus(status);
}

export interface DelegateBatchSummary {
    total: number;
    /** Stopped, whatever the outcome — see the job batch's "finished, not done". */
    finished: number;
    /** Stopped badly: errored, cancelled or refused. */
    failed: number;
}

export function summarizeDelegateBatch(
    statuses: readonly DelegateMemberStatus[],
): DelegateBatchSummary {
    let finished = 0;
    let failed = 0;
    for (const status of statuses) {
        if (!isLiveDelegateStatus(status)) finished++;
        if (delegateMemberTone(status) === 'danger') failed++;
    }
    return {total: statuses.length, finished, failed};
}
