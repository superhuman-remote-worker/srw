import {describe, expect, it} from 'vitest';
import {parseSubagentRecoveryContinuation, parseSubagentRecoveryResult} from './subagent-recovery.model';

/**
 * The marker a delegation-batch recovery stamps on the rows it writes
 * (`thread_messages.metrics.subagent_recovery`). Shapes copied from
 * `result_metrics` / `continuation_metrics` in
 * `src/shared/session_subagent_batch.py`.
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §5.4, §6.5.
 */

function resultMetrics(over: Record<string, unknown> = {}) {
    return {
        subagent_recovery: {
            version: 1,
            kind: 'result',
            class: 'interrupted',
            tool_call_id: 'call-1',
            thread_id: 'child-1',
            handle: 'explorer-1',
            subagent_type: 'explorer',
            subagent_status: 'interrupted',
            report_path: null,
            delivery_id: 'd-1',
            ...over,
        },
    };
}

function continuationMetrics(over: Record<string, unknown> = {}) {
    return {
        subagent_recovery: {
            version: 1,
            kind: 'continuation',
            supersedes_input_seq: 41,
            calls: 4,
            finished: 1,
            interrupted: 1,
            not_started: 2,
            declined: 0,
            retired: 0,
            ...over,
        },
    };
}

describe('parseSubagentRecoveryResult', () => {
    it('reads the class and the recorded child status', () => {
        expect(parseSubagentRecoveryResult(resultMetrics(), 'call-1')).toEqual({
            class: 'interrupted',
            subagentStatus: 'interrupted',
        });
    });

    it('reads every class the settle writes', () => {
        for (const cls of ['completed', 'interrupted', 'not_started', 'declined', 'retired']) {
            expect(parseSubagentRecoveryResult(resultMetrics({class: cls}), 'call-1')?.class).toBe(cls);
        }
    });

    it('has no child status for a call that never had a child', () => {
        const m = resultMetrics({class: 'not_started', thread_id: null, handle: null, subagent_status: null});
        expect(parseSubagentRecoveryResult(m, 'call-1')).toEqual({class: 'not_started', subagentStatus: null});
    });

    it('is null for any row without the marker', () => {
        for (const metrics of [null, undefined, {}, {usage: {input_tokens: 5}}, 'text', 7]) {
            expect(parseSubagentRecoveryResult(metrics, 'call-1')).toBeNull();
        }
    });

    it('is null for a marker this build does not know', () => {
        expect(parseSubagentRecoveryResult(resultMetrics({version: 2}), 'call-1')).toBeNull();
        expect(parseSubagentRecoveryResult(resultMetrics({class: 'vanished'}), 'call-1')).toBeNull();
        // The continuation's marker on a tool row is not a result.
        expect(parseSubagentRecoveryResult(continuationMetrics(), 'call-1')).toBeNull();
    });

    it('does not trust a marker that names another call', () => {
        expect(parseSubagentRecoveryResult(resultMetrics(), 'call-2')).toBeNull();
    });
});

describe('parseSubagentRecoveryContinuation', () => {
    it('reads the turn counts', () => {
        expect(parseSubagentRecoveryContinuation(continuationMetrics())).toEqual({
            calls: 4,
            finished: 1,
            interrupted: 1,
            notStarted: 2,
            declined: 0,
            retired: 0,
        });
    });

    it('is null without the marker, or with a result marker', () => {
        expect(parseSubagentRecoveryContinuation(null)).toBeNull();
        expect(parseSubagentRecoveryContinuation({})).toBeNull();
        expect(parseSubagentRecoveryContinuation(resultMetrics())).toBeNull();
    });

    it('is null when a count is missing or not a count', () => {
        expect(parseSubagentRecoveryContinuation(continuationMetrics({retired: undefined}))).toBeNull();
        expect(parseSubagentRecoveryContinuation(continuationMetrics({calls: '4'}))).toBeNull();
        expect(parseSubagentRecoveryContinuation(continuationMetrics({finished: -1}))).toBeNull();
        expect(parseSubagentRecoveryContinuation(continuationMetrics({version: 2}))).toBeNull();
    });
});
