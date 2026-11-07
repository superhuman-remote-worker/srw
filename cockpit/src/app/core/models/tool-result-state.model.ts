import type {ToolCardOutcome} from './tool-card.model';

/**
 * What a call's history says when it holds no result the call produced
 * (`knowledge-base/knowledge/issues/session_unknown_tool_result_needs_admin.md`):
 *
 * - `missing`: no result row at all. A lost process cut the call off and the
 *   owner has not decided yet, or (after a reload) it is still running.
 * - `unknown`: its result row says the result is unknown. The workspace was
 *   lost while it ran, or the owner decided on a cut-off call.
 * - `not_run`: its result row says it never started.
 *
 * The last two come from `thread_messages.metrics.tool_result_state`, and live
 * from the `result_state` of the call's `tool.completed` frame. Contract:
 * `src/shared/tool_result_state.py`.
 */
export type ToolResultState = 'missing' | 'unknown' | 'not_run';

/** A state a result row (or its `tool.completed` frame) can carry. */
export type MarkedToolResultState = Exclude<ToolResultState, 'missing'>;

const MARKED_STATES: ReadonlySet<string> = new Set(['unknown', 'not_run']);

/** The state a `tool.completed` frame's `result_state` names, or null. */
export function parseMarkedToolResultState(value: unknown): MarkedToolResultState | null {
    return typeof value === 'string' && MARKED_STATES.has(value) ? (value as MarkedToolResultState) : null;
}

/** The state a result row's marker names for this call, or null. */
export function parseToolResultState(metrics: unknown, toolCallId: string): MarkedToolResultState | null {
    if (!metrics || typeof metrics !== 'object') return null;
    const marker = (metrics as Record<string, unknown>)['tool_result_state'];
    if (!marker || typeof marker !== 'object') return null;
    const m = marker as Record<string, unknown>;
    if (m['tool_call_id'] !== toolCallId) return null;
    return parseMarkedToolResultState(m['state']);
}

/** The card's status pill for that state, in place of "completed". */
export function toolResultStateOutcome(state: ToolResultState | undefined): ToolCardOutcome | null {
    switch (state) {
        case 'missing':
            return {labelKey: 'toolCard.outcome.noResult', tone: 'warning'};
        case 'unknown':
            return {labelKey: 'toolCard.outcome.resultUnknown', tone: 'warning'};
        case 'not_run':
            return {labelKey: 'toolCard.outcome.notRun', tone: 'neutral'};
        default:
            return null;
    }
}
