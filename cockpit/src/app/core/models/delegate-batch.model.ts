import {BadgeTone} from '../../ui/badge/badge.component';
import {isLiveSubagentStatus, SUBAGENT_STATUSES, subagentStatusTone} from '../util/subagent-status';
import {JobSubagent, JobSubagentStatus} from './api.model';
import {SubagentRecoveryResult} from './subagent-recovery.model';
import {DELEGATE_TOOL, ToolCardOutcome, ToolCardStatus, ToolCardView} from './tool-card.model';
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
    /**
     * What a delegation-batch recovery found for this call, when its result
     * is one a recovery wrote (see `subagent-recovery.model.ts`). Absent
     * otherwise, including for a call the older per-child path recovered.
     */
    readonly recovery?: SubagentRecoveryResult;
    /** Render-ready card for the row's expandable body. */
    readonly view: ToolCardView;
}

/**
 * What a row's status pill says. The child lifecycle (`JobSubagentStatus`)
 * plus the states in which no child exists: the call is waiting on a
 * permission decision (`pending`), was refused (`denied`), its gate went
 * unanswered (`expired`), or a recovery settled it before it ever ran
 * (`not_started`).
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
            recovery: event.recovery,
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
 * - A result a recovery wrote decides first: the settle is the latest fact
 *   about the call, and its marker states the outcome — `interrupted`,
 *   `not_started`, `denied` for a declined call, `cancelled` for a child
 *   retired when the session was stopped. A `completed` class means the child
 *   ended and its report is the result: its terminal status (recorded by the
 *   settle, else the child row) says more than "completed" when it is
 *   `capped` or `error`. See {@link recoveredMemberStatus}.
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
 *   settles (WP1/WP2), not a queue. Once the recovery has written its result
 *   the marker above decides; until then, with no child row, the call keeps
 *   its own state.
 * - Once the call has returned, a terminal child row still wins (`capped`,
 *   `interrupted` say more than "completed"), but a stale non-terminal poll
 *   does not outvote the result.
 */
export function delegateMemberStatus(
    member: Pick<DelegateBatchMember, 'callStatus' | 'answered' | 'startedAt' | 'recovery'>,
    child: JobSubagent | null,
    rosterRequestedAt: number | null,
    superseded = false,
): DelegateMemberStatus {
    // A status this build does not know (a newer server) is treated as no
    // row, so the pill falls back to the call's own state, never a raw key.
    const childStatus = child && SUBAGENT_STATUSES.has(child.status) ? child.status : null;
    if (member.recovery) return recoveredMemberStatus(member.recovery, childStatus);
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
 * The status of a call whose result a delegation-batch recovery wrote, from
 * the marker on that row — never from its text. The live states cannot occur:
 * the settle ended every child it found running.
 */
export function recoveredMemberStatus(
    recovery: SubagentRecoveryResult,
    childStatus: JobSubagentStatus | null,
): DelegateMemberStatus {
    switch (recovery.class) {
        case 'interrupted':
            return 'interrupted';
        case 'not_started':
            return 'not_started';
        case 'declined':
            return 'denied';
        case 'retired':
            return 'cancelled';
        case 'completed': {
            // The child ended and its report is the result. A terminal status
            // other than `completed` (capped, error) says more; one this build
            // does not know says nothing.
            const terminal = [recovery.subagentStatus, childStatus].find(
                (s): s is JobSubagentStatus =>
                    !!s && SUBAGENT_STATUSES.has(s as JobSubagentStatus) && !isLiveSubagentStatus(s),
            );
            return terminal ?? 'completed';
        }
    }
}

/**
 * The pill of a `delegate_agent` call's ordinary tool card when a recovery
 * wrote its result — the lone call of a message, which renders outside any
 * batch card although the same settle covered it (§5.1). Same mapping, label
 * and tone as a batch row. Null when the card's own status already says it
 * ("OK" for a child that completed, "Denied" for a declined call) and for
 * every call without a marker, so those render exactly as before.
 */
export function recoveredDelegateCallOutcome(
    event: Pick<ToolCallEvent, 'tool' | 'recovery'>,
): ToolCardOutcome | null {
    if (event.tool !== DELEGATE_TOOL || !event.recovery) return null;
    const status = recoveredMemberStatus(event.recovery, null);
    if (status === 'completed' || status === 'denied') return null;
    return {labelKey: DELEGATE_STATUS_LABEL_KEYS[status], tone: delegateMemberTone(status)};
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
    /**
     * Stopped short, and never run at all. Both count as finished too; they
     * get their own chips because "4/4 finished" alone reads a batch a
     * recovery settled as a clean one.
     */
    interrupted: number;
    notStarted: number;
}

export function summarizeDelegateBatch(
    statuses: readonly DelegateMemberStatus[],
): DelegateBatchSummary {
    let finished = 0;
    let failed = 0;
    let interrupted = 0;
    let notStarted = 0;
    for (const status of statuses) {
        if (!isLiveDelegateStatus(status)) finished++;
        if (delegateMemberTone(status) === 'danger') failed++;
        if (status === 'interrupted') interrupted++;
        if (status === 'not_started') notStarted++;
    }
    return {total: statuses.length, finished, failed, interrupted, notStarted};
}
