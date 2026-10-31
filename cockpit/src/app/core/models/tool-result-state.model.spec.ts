import {describe, expect, it} from 'vitest';
import {ToolCallEvent} from './turn.model';
import {parseToolResultState, toolResultStateOutcome} from './tool-result-state.model';
import {toolCardViewFromEvent} from '../tools/tool-card-adapters';

// A call whose history holds no result it produced must not read "completed"
// (session_unknown_tool_result_needs_admin.md). Contract of the marker:
// src/shared/tool_result_state.py.

const call = (over: Partial<ToolCallEvent> = {}): ToolCallEvent => ({
    kind: 'tool_call',
    id: 'c1',
    tool: 'shell_execute',
    args: {command: 'sleep 240'},
    status: 'completed',
    startedAt: 0,
    ...over,
});

describe('parseToolResultState', () => {
    it('reads the state the marker names for this call', () => {
        const metrics = {tool_result_state: {state: 'unknown', tool_call_id: 'c1'}};
        expect(parseToolResultState(metrics, 'c1')).toBe('unknown');
        expect(parseToolResultState({tool_result_state: {state: 'not_run', tool_call_id: 'c1'}}, 'c1')).toBe(
            'not_run',
        );
    });

    it('ignores a marker of another call, an unknown state, or none', () => {
        expect(parseToolResultState({tool_result_state: {state: 'unknown', tool_call_id: 'c2'}}, 'c1')).toBeNull();
        expect(parseToolResultState({tool_result_state: {state: 'finished', tool_call_id: 'c1'}}, 'c1')).toBeNull();
        expect(parseToolResultState({input_tokens: 4}, 'c1')).toBeNull();
        expect(parseToolResultState(null, 'c1')).toBeNull();
    });
});

describe('toolResultStateOutcome', () => {
    it('labels each state instead of "completed"', () => {
        expect(toolResultStateOutcome('missing')).toEqual({labelKey: 'toolCard.outcome.noResult', tone: 'warning'});
        expect(toolResultStateOutcome('unknown')).toEqual({
            labelKey: 'toolCard.outcome.resultUnknown',
            tone: 'warning',
        });
        expect(toolResultStateOutcome('not_run')).toEqual({labelKey: 'toolCard.outcome.notRun', tone: 'neutral'});
        expect(toolResultStateOutcome(undefined)).toBeNull();
    });
});

describe('toolCardViewFromEvent — a call without a result it produced', () => {
    it('shows the state in the pill', () => {
        expect(toolCardViewFromEvent(call({resultState: 'missing'})).outcome?.labelKey).toBe(
            'toolCard.outcome.noResult',
        );
        expect(toolCardViewFromEvent(call({resultState: 'unknown', result: 'Result unknown: …'})).outcome?.labelKey).toBe(
            'toolCard.outcome.resultUnknown',
        );
    });

    it('keeps an ordinary completed call as it was', () => {
        const view = toolCardViewFromEvent(call({result: 'done'}));
        expect(view.status).toBe('ok');
        expect(view.outcome).toBeUndefined();
    });

    it('shows a call the session reports running as running', () => {
        const view = toolCardViewFromEvent(call({resultState: 'missing'}), {running: true});
        expect(view.status).toBe('running');
        expect(view.outcome).toBeUndefined();
    });
});
