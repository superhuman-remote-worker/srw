import {describe, expect, it} from 'vitest';

import type {AssistantTurn} from '../models/turn.model';
import {
    CitationScope,
    citationIdsIn,
    citationScopeFor,
    holdBackPartialHandle,
} from './citation-scope';

function turn(events: AssistantTurn['events']): AssistantTurn {
    return {kind: 'assistant', id: 't1', events, status: 'done', startedAt: 0} as AssistantTurn;
}

const text = (id: string, content: string) =>
    ({kind: 'text', id, content, status: 'done', startedAt: 0}) as AssistantTurn['events'][number];
const thought = (id: string, content: string) =>
    ({kind: 'thought', id, content, status: 'done', startedAt: 0}) as AssistantTurn['events'][number];

describe('citationIdsIn', () => {
    it('lists handles and legacy markers in first-mention order', () => {
        expect(citationIdsIn('a [^c7] b [12] c [^c7] d [c3]')).toEqual([7, 12, 3]);
    });

    it('skips links, definitions and code', () => {
        expect(citationIdsIn('[1](http://x) [2][ref] [^c3]: def `[^c4]`\n```\n[^c5]\n```\n')).toEqual([]);
    });
});

describe('CitationScope', () => {
    it('numbers known citations by first mention and skips unknown ones', () => {
        const scope = new CitationScope([7, 5, 3]);
        const known = new Map([[7, {}], [3, {}]]);
        expect(scope.numberOf(7, known)).toBe(1);
        expect(scope.numberOf(3, known)).toBe(2);
        expect(scope.numberOf(5, known)).toBeUndefined();
    });

    it('gives a handle outside the answer text the next number', () => {
        const scope = new CitationScope([7]);
        const known = new Map([[7, {}], [9, {}]]);
        expect(scope.numberOf(9, known)).toBe(2);
        expect(scope.numberOf(9, known)).toBe(2);
    });

    it('renumbers when a citation arrives', () => {
        const scope = new CitationScope([5, 7]);
        expect(scope.numberOf(7, new Map([[7, {}]]))).toBe(1);
        expect(scope.numberOf(7, new Map([[5, {}], [7, {}]]))).toBe(2);
    });
});

describe('citationScopeFor', () => {
    it('numbers across the whole answer: text → tool → text does not restart', () => {
        const t = turn([
            text('a', 'First claim [^c40].'),
            {kind: 'tool_call', id: 'x'} as AssistantTurn['events'][number],
            text('b', 'Second claim [^c41], again [^c40].'),
            thought('c', 'Thinking about [^c42].'),
        ]);
        const scope = citationScopeFor(t);
        expect(scope.order).toEqual([40, 41, 42]);
        expect(citationScopeFor(t)).toBe(scope);
    });
});

describe('holdBackPartialHandle', () => {
    it('drops a half-written handle at the end of streaming text only', () => {
        expect(holdBackPartialHandle('claim [^c12', true)).toBe('claim ');
        expect(holdBackPartialHandle('claim [^', true)).toBe('claim ');
        expect(holdBackPartialHandle('claim [^c12]', true)).toBe('claim [^c12]');
        expect(holdBackPartialHandle('claim [^c12', false)).toBe('claim [^c12');
    });
});
