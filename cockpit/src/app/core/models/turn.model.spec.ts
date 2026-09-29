import {describe, expect, it} from 'vitest';
import {
    AssistantTurn,
    collapsedAnswer,
    CompactionEvent,
    EventGroup,
    firstSentence,
    firstTextOf,
    foldWakeCycles,
    FoldableEvent,
    groupEvents,
    isFollowedByAnswer,
    isQuietWakeTurn,
    isSessionBoundary,
    isSitrepTurn,
    lastTextOf,
    lastTurnOf,
    nextSpeakingTurn,
    notifyToolCalls,
    sleepRequest,
    summarizeFolded,
    TextEvent,
    ThoughtEvent,
    ToolCallEvent,
    ToolCallStatus,
    trailingText,
    Turn,
    TurnEvent,
} from './turn.model';
import {SLEEP_TOOL} from './tool-card.model';

function mkTurn(events: AssistantTurn['events']): AssistantTurn {
    return {kind: 'assistant', id: 't1', events, status: 'done', startedAt: 0};
}
const txt = (id: string, content: string): TextEvent => ({kind: 'text', id, content, status: 'done', startedAt: 0});
const tht = (id: string, status: ThoughtEvent['status'] = 'done'): ThoughtEvent =>
    ({kind: 'thought', id, content: '...', status, startedAt: 0});
const tool = (id: string, status: ToolCallStatus = 'completed', category?: string): ToolCallEvent =>
    ({kind: 'tool_call', id, tool: 'read_file', args: {}, status, startedAt: 0, category});
const notify = (id: string): ToolCallEvent =>
    ({kind: 'tool_call', id, tool: 'notify_user', args: {message: 'm', urgency: 'log'}, status: 'completed', startedAt: 0});
const job = (id: string, status: ToolCallStatus = 'completed'): ToolCallEvent =>
    ({kind: 'tool_call', id, tool: 'create_job', args: {description: 'd'}, status, startedAt: 0});
const comp = (id: string): CompactionEvent => ({kind: 'compaction', id, summary: 'compacted', startedAt: 0});
/** A delegate_agent call carried by assistant message `msg`. */
const dlg = (id: string, msg: string | undefined, status: ToolCallStatus = 'completed'): ToolCallEvent =>
    ({kind: 'tool_call', id, tool: 'delegate_agent', args: {subagent_type: 'explorer', description: id},
        status, startedAt: 0, messageKey: msg});

/** Ids in a group, folded / batched / single — keeps the assertions readable. */
const idsOf = (g: EventGroup): string[] =>
    g.kind === 'single' ? [g.event.id] : g.events.map(e => e.id);
const shapeOf = (gs: EventGroup[]) => gs.map(g => `${g.kind}(${idsOf(g).join(',')})`);

describe('firstTextOf', () => {
    it('returns the first text event, skipping leading thoughts', () => {
        const t = mkTurn([tht('b0'), txt('b1', 'first'), txt('b2', 'second')]);
        expect(firstTextOf(t)?.content).toBe('first');
    });

    it('returns undefined when the turn has no text', () => {
        expect(firstTextOf(mkTurn([tht('b0')]))).toBeUndefined();
    });

    it('is the opposite end from lastTextOf', () => {
        const t = mkTurn([txt('b0', 'first'), txt('b1', 'last')]);
        expect(firstTextOf(t)?.content).toBe('first');
        expect(lastTextOf(t)?.content).toBe('last');
    });
});

describe('firstSentence', () => {
    it('returns the first period-terminated sentence', () => {
        expect(firstSentence('I will read the file. Then edit it.')).toBe('I will read the file.');
    });

    it('strips a leading markdown heading marker', () => {
        expect(firstSentence('## Plan\nDo the thing.')).toBe('Plan Do the thing.');
    });

    it('strips an opening and closing code fence', () => {
        expect(firstSentence('```python\ndef greet():\n    pass\n```')).toBe('def greet(): pass');
    });

    it('strips the fence even when no closing fence is present', () => {
        expect(firstSentence('```ts\nconst x = 1')).toBe('const x = 1');
    });

    it('collapses internal whitespace and newlines', () => {
        expect(firstSentence('Looking   into\nthis now.')).toBe('Looking into this now.');
    });

    it('caps an over-long sentence with an ellipsis', () => {
        const out = firstSentence('x'.repeat(200), 140);
        expect(out).toHaveLength(140);
        expect(out.endsWith('…')).toBe(true);
    });

    it('returns empty string for blank or empty input', () => {
        expect(firstSentence('   ')).toBe('');
        expect(firstSentence('')).toBe('');
    });
});

describe('trailingText', () => {
    it('returns the closing prose after the last tool call', () => {
        const t = mkTurn([txt('b0', 'plan'), tool('b1'), txt('b2', 'Here is the result.')]);
        expect(trailingText(t)).toBe('Here is the result.');
    });

    it('joins multiple trailing text blocks with a blank line', () => {
        const t = mkTurn([tool('b0'), txt('b1', 'First.'), txt('b2', 'Second.')]);
        expect(trailingText(t)).toBe('First.\n\nSecond.');
    });

    it('returns "" when the turn ends on a tool call (no closing prose)', () => {
        expect(trailingText(mkTurn([txt('b0', 'doing'), tool('b1')]))).toBe('');
    });

    it('skips a stray finished thought trailing the closing prose', () => {
        // Transports that only hand reasoning over post-stream broadcast the
        // thought AFTER the answer tokens — it must not blank the answer.
        expect(trailingText(mkTurn([txt('b0', 'hi'), tht('b1')]))).toBe('hi');
    });

    it('skips multiple trailing finished/hidden thoughts', () => {
        const t = mkTurn([tool('b0'), txt('b1', 'answer.'), tht('b2'), tht('b3', 'hidden')]);
        expect(trailingText(t)).toBe('answer.');
    });

    it('returns "" when the trailing thought is still streaming (mid-work)', () => {
        expect(trailingText(mkTurn([txt('b0', 'so far'), tht('b1', 'streaming')]))).toBe('');
    });

    it('skips a trailing inline compaction marker', () => {
        expect(trailingText(mkTurn([txt('b0', 'answer.'), comp('b1')]))).toBe('answer.');
    });

    it('still cuts the run at a thought BETWEEN texts (earlier text is lead-up)', () => {
        const t = mkTurn([txt('b0', 'lead'), tht('b1'), txt('b2', 'final.'), tht('b3')]);
        expect(trailingText(t)).toBe('final.');
    });

    it('returns "" when a tool call follows the last text, even behind a thought', () => {
        expect(trailingText(mkTurn([txt('b0', 'doing'), tool('b1'), tht('b2')]))).toBe('');
    });

    it('returns "" for an all-thought turn', () => {
        expect(trailingText(mkTurn([tht('b0'), tht('b1')]))).toBe('');
    });

    it('returns the whole text for an all-text turn', () => {
        expect(trailingText(mkTurn([txt('b0', 'only answer.')]))).toBe('only answer.');
    });

    it('returns "" for an empty turn', () => {
        expect(trailingText(mkTurn([]))).toBe('');
    });

    it('keeps only the final text run, folding earlier interleaved text', () => {
        const t = mkTurn([txt('b0', 'lead'), tool('b1'), txt('b2', 'mid'), tool('b3'), txt('b4', 'final.')]);
        expect(trailingText(t)).toBe('final.');
    });
});

describe('collapsedAnswer', () => {
    it('returns the closing prose when the turn ends on text', () => {
        const t = mkTurn([txt('b0', 'plan'), tool('b1'), txt('b2', 'Here is the result.')]);
        expect(collapsedAnswer(t)).toBe('Here is the result.');
    });

    it('recovers the answer when a completed citation pass trails the text', () => {
        // The model writes its answer, then registers citations — the turn ends
        // on cite_web tool calls, so trailingText() is empty. The answer must
        // still surface instead of collapsing to a one-line opening headline.
        const t = mkTurn([
            txt('b0', 'I will build the pack.'),
            tool('b1'),
            txt('b2', 'Done — here is the finished pack.'),
            tool('b3', 'completed', 'citation'),
            tool('b4', 'completed', 'citation'),
        ]);
        expect(collapsedAnswer(t)).toBe('Done — here is the finished pack.');
    });

    it('joins the trailing text run behind the trailing tool calls', () => {
        const t = mkTurn([txt('b0', 'First.'), txt('b1', 'Second.'), tool('b2', 'completed', 'citation')]);
        expect(collapsedAnswer(t)).toBe('First.\n\nSecond.');
    });

    it('recovers the answer regardless of the trailing tool category', () => {
        // Not just citations — a closing verification run_command, a final save,
        // etc.; history rows may also lack a stamped category entirely.
        const t = mkTurn([txt('b0', 'The file is valid.'), tool('b1')]);
        expect(collapsedAnswer(t)).toBe('The file is valid.');
    });

    it('returns "" while a trailing tool call is still running (mid-work)', () => {
        expect(collapsedAnswer(mkTurn([txt('b0', 'working'), tool('b1', 'running')]))).toBe('');
    });

    it('returns "" while a trailing thought is still streaming (mid-work)', () => {
        expect(collapsedAnswer(mkTurn([txt('b0', 'so far'), tht('b1', 'streaming')]))).toBe('');
    });

    it('returns "" when the turn has no text at all', () => {
        expect(collapsedAnswer(mkTurn([tool('b0', 'completed', 'citation'), tht('b1')]))).toBe('');
    });

    it('cuts the run at a tool BETWEEN texts (earlier text is lead-up)', () => {
        const t = mkTurn([txt('b0', 'lead'), tool('b1'), txt('b2', 'final.'), tool('b3', 'completed', 'citation')]);
        expect(collapsedAnswer(t)).toBe('final.');
    });
});

describe('groupEvents', () => {
    it('returns [] for no events', () => {
        expect(groupEvents([])).toEqual([]);
    });

    it('MERGES tool runs across an interleaved thought — the whole point', () => {
        // The old rule broke a run at every thought, so a turn that alternated
        // thought/tools/thought/tools rendered as a dozen rows even though each
        // run folded. Only the latest tool (b4) stays pinned.
        const g = groupEvents([tool('b0'), tool('b1'), tht('b2'), tool('b3'), tool('b4')]);
        expect(shapeOf(g)).toEqual(['folded(b0,b1,b2,b3)', 'single(b4)']);
    });

    it('never folds text — it is the answer', () => {
        // b4 is the latest tool call so it pins; b1..b3 fold between the two texts.
        const g = groupEvents([txt('b0', 'plan'), tool('b1'), tool('b2'), tool('b3'), tool('b4'), txt('b5', 'done')]);
        expect(shapeOf(g)).toEqual(['single(b0)', 'folded(b1,b2,b3)', 'single(b4)', 'single(b5)']);
    });

    it('never folds a compaction marker', () => {
        const comp = {kind: 'compaction', id: 'b2', summary: 's', startedAt: 0} as const;
        const g = groupEvents([tool('b0'), tht('b1'), comp, tool('b3'), tool('b4'), tool('b5')]);
        expect(shapeOf(g)).toEqual(['folded(b0,b1)', 'single(b2)', 'folded(b3,b4)', 'single(b5)']);
    });

    it('never folds notify_user — an officer→user message is first-class, like text', () => {
        // Buried mid-run between plain tool calls, the message must still
        // surface as its own bubble rather than vanish into the chip.
        const g = groupEvents([tool('b0'), tool('b1'), notify('b2'), tool('b3'), tool('b4'), tool('b5')]);
        expect(shapeOf(g)).toEqual(['folded(b0,b1)', 'single(b2)', 'folded(b3,b4)', 'single(b5)']);
    });

    describe('the live edge', () => {
        it('pins every in-flight call — 5 tools at once means 5 cards', () => {
            const g = groupEvents([
                tool('b0'), tool('b1', 'running'), tool('b2', 'running'),
                tool('b3', 'running'), tool('b4', 'running'), tool('b5', 'pending'),
            ]);
            // b0 alone is below MIN_FOLD_RUN, so it renders inline rather than chipping.
            expect(shapeOf(g)).toEqual([
                'single(b0)', 'single(b1)', 'single(b2)', 'single(b3)', 'single(b4)', 'single(b5)',
            ]);
        });

        it('drops each call into the chip as it completes', () => {
            const g = groupEvents([
                tool('b0'), tool('b1'), tool('b2'), tool('b3', 'running'), tool('b4', 'running'),
            ]);
            expect(shapeOf(g)).toEqual(['folded(b0,b1,b2)', 'single(b3)', 'single(b4)']);
        });

        it('pins a streaming thought', () => {
            // No tool call here, so nothing competes for the latest-tool pin.
            const g = groupEvents([tht('b0'), tht('b1'), tht('b2'), tht('b3', 'streaming')]);
            expect(shapeOf(g)).toEqual(['folded(b0,b1,b2)', 'single(b3)']);
        });

        it('always pins the latest tool call, even in a finished turn', () => {
            const g = groupEvents([tht('b0'), tool('b1'), tht('b2'), tool('b3')]);
            expect(shapeOf(g)).toEqual(['folded(b0,b1,b2)', 'single(b3)']);
        });

        it('pins only the LAST tool call, not every trailing one', () => {
            const g = groupEvents([tool('b0'), tool('b1'), tool('b2')]);
            expect(shapeOf(g)).toEqual(['folded(b0,b1)', 'single(b2)']);
        });

        it('handles out-of-order completion without reordering', () => {
            // b1 still running while b2/b3 already returned: b1 pinned (in flight),
            // b3 pinned (latest), b2 stranded between them — renders inline, in place.
            const g = groupEvents([tool('b0'), tool('b1', 'running'), tool('b2'), tool('b3')]);
            expect(shapeOf(g)).toEqual(['single(b0)', 'single(b1)', 'single(b2)', 'single(b3)']);
        });

        it('folds a thought-only turn with no tool call to pin', () => {
            const g = groupEvents([tht('b0'), tht('b1'), tht('b2')]);
            expect(shapeOf(g)).toEqual(['folded(b0,b1,b2)']);
        });
    });

    it('renders a sub-threshold run inline instead of chipping it', () => {
        // A "1× thought" chip is the same height as the card and less informative.
        const g = groupEvents([tht('b0'), tool('b1', 'running')]);
        expect(shapeOf(g)).toEqual(['single(b0)', 'single(b1)']);
    });

    it('ids the group by its first member so @for track stays stable', () => {
        const g = groupEvents([tool('b0'), tht('b1'), tool('b2')]);
        expect(g[0].id).toBe('b0');
    });

    describe('job fan-out', () => {
        it('groups contiguous job calls into one batch', () => {
            expect(shapeOf(groupEvents([job('b0'), job('b1'), job('b2')])))
                .toEqual(['job_batch(b0,b1,b2)']);
        });

        it('FIXES the fan-out that used to hide two of three job cards', () => {
            // Before: job calls were foldable and pinnedEventIds() pins only the
            // turn's LAST tool call, so this rendered as 'folded(b0,b1)' plus
            // 'single(b2)' — two actionable cards buried in a "2× tool calls"
            // chip that gave no hint they were there. The regression this guards
            // is re-adding create_job to isFoldable().
            const g = groupEvents([job('b0'), job('b1'), job('b2')]);
            expect(g.map(x => x.kind)).not.toContain('folded');
            expect(g).toHaveLength(1);
        });

        it('leaves a lone job call as an ordinary card', () => {
            // A one-row batch is a card with a redundant header.
            expect(shapeOf(groupEvents([job('b0')]))).toEqual(['single(b0)']);
        });

        it('never folds a job call buried in a run of ordinary tools', () => {
            // The notify_user case, but for an actionable card. b5 is the latest
            // tool call and pins; b2 stands alone because it can never fold.
            const g = groupEvents([tool('b0'), tool('b1'), job('b2'), tool('b3'), tool('b4'), tool('b5')]);
            expect(shapeOf(g)).toEqual(['folded(b0,b1)', 'single(b2)', 'folded(b3,b4)', 'single(b5)']);
        });

        it('breaks the batch on anything the agent said or did between dispatches', () => {
            const g = groupEvents([job('b0'), job('b1'), txt('b2', 'and now'), job('b3'), job('b4')]);
            expect(shapeOf(g)).toEqual(['job_batch(b0,b1)', 'single(b2)', 'job_batch(b3,b4)']);
        });

        it('batches while still in flight, preserving order', () => {
            const g = groupEvents([job('b0'), job('b1', 'running'), job('b2', 'pending')]);
            expect(shapeOf(g)).toEqual(['job_batch(b0,b1,b2)']);
        });

        it('ids the batch by its first member so @for track stays stable', () => {
            expect(groupEvents([job('b0'), job('b1')])[0].id).toBe('b0');
        });

        it('does not disturb a turn with no job calls', () => {
            const g = groupEvents([tool('b0'), tool('b1'), tool('b2')]);
            expect(shapeOf(g)).toEqual(['folded(b0,b1)', 'single(b2)']);
        });
    });

    describe('delegate fan-out', () => {
        it('groups the delegate calls of one message into one batch', () => {
            expect(shapeOf(groupEvents([dlg('b0', 'm1'), dlg('b1', 'm1'), dlg('b2', 'm1')])))
                .toEqual(['delegate_batch(b0,b1,b2)']);
        });

        it('batches while in flight and after the results land, the same way', () => {
            const running = groupEvents([dlg('b0', 'm1', 'running'), dlg('b1', 'm1', 'running')]);
            const done = groupEvents([dlg('b0', 'm1'), dlg('b1', 'm1')]);
            expect(shapeOf(running)).toEqual(['delegate_batch(b0,b1)']);
            expect(shapeOf(done)).toEqual(['delegate_batch(b0,b1)']);
        });

        it('never folds a finished batch into a chip', () => {
            // pinnedEventIds() pins only the turn's LAST call once nothing is in
            // flight; without the batch this rendered 'folded(b0,b1)' plus
            // 'single(b2)' — two children buried in a "2× delegations" chip.
            const g = groupEvents([dlg('b0', 'm1'), dlg('b1', 'm1'), dlg('b2', 'm1')]);
            expect(g.map(x => x.kind)).not.toContain('folded');
        });

        it('keeps a single delegation exactly as before: a card, and foldable once finished', () => {
            // In flight or latest: pinned, an ordinary card.
            expect(shapeOf(groupEvents([dlg('b0', 'm1', 'running')]))).toEqual(['single(b0)']);
            expect(shapeOf(groupEvents([tool('b0'), dlg('b1', 'm2')]))).toEqual(['single(b0)', 'single(b1)']);
            // Finished and not the latest call: folds with its neighbours, as
            // any finished tool call does.
            expect(shapeOf(groupEvents([tool('b0'), dlg('b1', 'm2'), tool('b2'), tool('b3')])))
                .toEqual(['folded(b0,b1,b2)', 'single(b3)']);
        });

        it('keeps two back-to-back single delegations apart', () => {
            // Two responses of one call each, nothing between them: two
            // ordinary cards, not a batch of two.
            const g = groupEvents([dlg('b0', 'm1', 'running'), dlg('b1', 'm2', 'running')]);
            expect(shapeOf(g)).toEqual(['single(b0)', 'single(b1)']);
        });

        it('splits back-to-back fan-outs by message', () => {
            const g = groupEvents([dlg('b0', 'm1'), dlg('b1', 'm1'), dlg('b2', 'm2'), dlg('b3', 'm2')]);
            expect(shapeOf(g)).toEqual(['delegate_batch(b0,b1)', 'delegate_batch(b2,b3)']);
        });

        it('leaves the lone call of a message next to a fan-out as an ordinary card', () => {
            const g = groupEvents([dlg('b0', 'm1'), dlg('b1', 'm2'), dlg('b2', 'm2')]);
            expect(shapeOf(g)).toEqual(['single(b0)', 'delegate_batch(b1,b2)']);
        });

        it('breaks the batch on anything between two calls', () => {
            const g = groupEvents([dlg('b0', 'm1'), tool('b1'), dlg('b2', 'm1')]);
            expect(g.map(x => x.kind)).not.toContain('delegate_batch');
        });

        it('falls back to contiguity for calls without a message key', () => {
            expect(shapeOf(groupEvents([dlg('b0', undefined), dlg('b1', undefined)])))
                .toEqual(['delegate_batch(b0,b1)']);
        });

        it('does not disturb a job batch beside it', () => {
            const g = groupEvents([job('b0'), job('b1'), dlg('b2', 'm2'), dlg('b3', 'm2')]);
            expect(shapeOf(g)).toEqual(['job_batch(b0,b1)', 'delegate_batch(b2,b3)']);
        });

        it('ids the batch by its first member so @for track stays stable', () => {
            expect(groupEvents([dlg('b0', 'm1'), dlg('b1', 'm1')])[0].id).toBe('b0');
        });
    });

    describe('isFollowedByAnswer (is a fan-out without results still running?)', () => {
        const turnOf = (id: string, events: TurnEvent[]): AssistantTurn =>
            ({kind: 'assistant', id, events, status: 'done', startedAt: 0});
        const user: Turn = {kind: 'user', id: 'u', content: 'hi', timestamp: 0};
        const notice: Turn = {kind: 'system', id: 's', content: 'resumed', timestamp: 0};

        it('is false while nothing but user and system rows follow the batch', () => {
            // Input typed during a batch, or a recovery notice, is no answer.
            const t = turnOf('t1', [txt('b0', 'on it'), dlg('b1', 'm1'), dlg('b2', 'm1')]);
            expect(isFollowedByAnswer([user, t, user, notice], t, 'b2')).toBe(false);
        });

        it('is true when prose follows in the same turn', () => {
            // A stateless recovery reuses the abandoned turn number, so its
            // answer lands in the same bubble.
            const t = turnOf('t1', [dlg('b1', 'm1'), dlg('b2', 'm1'), txt('b3', 'done')]);
            expect(isFollowedByAnswer([t], t, 'b2')).toBe(true);
        });

        it('is true when a later assistant turn has prose', () => {
            const t = turnOf('t1', [dlg('b1', 'm1'), dlg('b2', 'm1')]);
            const later = turnOf('t2', [txt('c0', 'answer')]);
            expect(isFollowedByAnswer([t, user, later], t, 'b2')).toBe(true);
        });

        it('ignores prose before the batch', () => {
            const t = turnOf('t1', [txt('b0', 'plan'), dlg('b1', 'm1'), dlg('b2', 'm1')]);
            expect(isFollowedByAnswer([t], t, 'b2')).toBe(false);
        });
    });

    it('collapses the screenshot case to one chip plus the live edge', () => {
        // The reported shape: text, then thought/tools alternating forever.
        const events = [txt('b0', 'analysis')];
        for (let i = 1; i <= 12; i++) {
            events.push(i % 2 ? tht(`b${i}`) : tool(`b${i}`));
        }
        const g = groupEvents(events);
        expect(g.map(x => x.kind)).toEqual(['single', 'folded', 'single']);
    });
});

describe('notifyToolCalls', () => {
    it('returns only the notify_user calls, in order', () => {
        const t = mkTurn([tool('b0'), notify('b1'), txt('b2', 'closing'), notify('b3')]);
        expect(notifyToolCalls(t).map(e => e.id)).toEqual(['b1', 'b3']);
    });

    it('returns [] for a turn without officer messages', () => {
        expect(notifyToolCalls(mkTurn([tool('b0'), txt('b1', 'x')]))).toEqual([]);
    });
});

describe('summarizeFolded', () => {
    const sum = (evts: FoldableEvent[]) => summarizeFolded(evts);

    it('counts by category, with thoughts as their own bucket', () => {
        const s = sum([
            tool('a', 'completed', 'research'), tool('b', 'completed', 'research'),
            tool('c', 'completed', 'citation'), tht('d'),
        ]);
        expect(s.parts).toEqual([
            {category: 'research', count: 2},
            {category: 'citation', count: 1},
            {category: 'thought', count: 1},
        ]);
    });

    it('sorts by count, highest first', () => {
        const s = sum([
            tool('a', 'completed', 'shell'),
            tool('b', 'completed', 'research'), tool('c', 'completed', 'research'),
            tool('d', 'completed', 'research'),
        ]);
        expect(s.parts.map(p => p.category)).toEqual(['research', 'shell']);
    });

    it('keeps first-appearance order for ties', () => {
        const s = sum([tool('a', 'completed', 'git'), tool('b', 'completed', 'shell')]);
        expect(s.parts.map(p => p.category)).toEqual(['git', 'shell']);
    });

    it('buckets calls with no category as "other" (older history rows)', () => {
        expect(sum([tool('a')]).parts).toEqual([{category: 'other', count: 1}]);
    });

    it('counts failures for the chip badge', () => {
        const s = sum([
            tool('a', 'completed', 'shell'), tool('b', 'error', 'shell'),
            tool('c', 'denied', 'shell'), tht('d'),
        ]);
        expect(s.failed).toBe(2);
    });

    it('counts a resultStatus error even when status looks completed', () => {
        const e: ToolCallEvent = {...tool('a', 'completed', 'shell'), resultStatus: 'error'};
        expect(sum([e]).failed).toBe(1);
    });

    it('never counts a thought as failed', () => {
        expect(sum([tht('a'), tht('b')]).failed).toBe(0);
    });

    it('does not double-count: parts sum to the event count', () => {
        // The reason this is categorical and has no grand total — a total plus
        // per-category counts would count every command twice.
        const evts = [
            tool('a', 'completed', 'shell'), tool('b', 'completed', 'research'), tht('c'),
        ];
        expect(sum(evts).parts.reduce((n, p) => n + p.count, 0)).toBe(evts.length);
    });
});

describe('officer log lens', () => {
    const sys = (id: string, content: string): Turn =>
        ({kind: 'system', id, content, timestamp: 1000} as unknown as Turn);
    const asst = (id: string, events: TurnEvent[], status = 'done'): Turn =>
        ({kind: 'assistant', id, events, status, timestamp: 2000} as unknown as Turn);
    const call = (id: string, tool: string, args: Record<string, unknown> = {}): TurnEvent =>
        ({kind: 'tool_call', id, tool, args, status: 'completed', startedAt: 0} as unknown as TurnEvent);
    const thought = (id: string): TurnEvent =>
        ({kind: 'thought', id, content: 'hm', status: 'done'} as unknown as TurnEvent);
    const text = (id: string): TurnEvent =>
        ({kind: 'text', id, content: 'Nothing changed.', status: 'done'} as unknown as TurnEvent);
    const sleep = (id: string, minutes = 30, reason = 'all jobs terminal') =>
        call(id, SLEEP_TOOL, {minutes, reason});
    const shape = (views: ReturnType<typeof foldWakeCycles>) =>
        views.map((v) => (v.kind === 'wake_cycle' ? `cycle(${v.sitrep.id}+${v.wake.id})` : `turn(${v.id})`));

    it('recognises a sitrep by its prefix only', () => {
        expect(isSitrepTurn(sys('s', '[SITREP] 2026-08-19 06:11 UTC — delta'))).toBe(true);
        expect(isSitrepTurn(sys('s', '  [SITREP] leading space'))).toBe(true);
        expect(isSitrepTurn(sys('s', 'Session resumed'))).toBe(false);
        expect(isSitrepTurn(asst('a', [sleep('c')]))).toBe(false);
    });

    it('a quiet wake is thoughts, text and a sleep — nothing else', () => {
        expect(isQuietWakeTurn(asst('a', [thought('t'), text('x'), sleep('c')]))).toBe(true);
        expect(isQuietWakeTurn(asst('a', [thought('t')]))).toBe(false); // never slept
        expect(isQuietWakeTurn(asst('a', [call('j', 'create_job'), sleep('c')]))).toBe(false);
        expect(isQuietWakeTurn(asst('a', [call('n', 'notify_user'), sleep('c')]))).toBe(false);
        expect(isQuietWakeTurn(asst('a', [sleep('c')], 'streaming'))).toBe(false);
    });

    it('reads the sleep request off the last sleep call', () => {
        expect(sleepRequest(asst('a', [sleep('c', 45, 'waiting on migration')]) as never))
            .toEqual({minutes: 45, reason: 'waiting on migration'});
        expect(sleepRequest(asst('a', [call('c', SLEEP_TOOL, {minutes: '10'})]) as never))
            .toEqual({minutes: 10, reason: ''});
        expect(sleepRequest(asst('a', [call('c', SLEEP_TOOL, {})]) as never))
            .toEqual({minutes: null, reason: ''});
    });

    it('folds sitrep + quiet wake into one cycle, leaves everything else alone', () => {
        const turns = [
            sys('s1', '[SITREP] one'),
            asst('a1', [thought('t1'), sleep('c1')]),
            sys('s2', '[SITREP] two'),
            asst('a2', [call('j', 'create_job'), sleep('c2')]),
            sys('r', 'Session resumed'),
            sys('s3', '[SITREP] three'),
            asst('a3', [sleep('c3')]),
        ];
        expect(shape(foldWakeCycles(turns))).toEqual([
            'cycle(s1+a1)',
            'turn(s2)',
            'turn(a2)',
            'turn(r)',
            'cycle(s3+a3)',
        ]);
    });

    it('a sitrep with no completed wake behind it stays a plain turn', () => {
        expect(shape(foldWakeCycles([sys('s1', '[SITREP] one')]))).toEqual(['turn(s1)']);
        expect(shape(foldWakeCycles([sys('s1', '[SITREP] one'), asst('a1', [sleep('c')], 'streaming')])))
            .toEqual(['turn(s1)', 'turn(a1)']);
    });

    it('carries the sleep request on the cycle', () => {
        const [cycle] = foldWakeCycles([sys('s', '[SITREP] x'), asst('a', [sleep('c', 20, 'quiet')])]);
        expect(cycle.kind).toBe('wake_cycle');
        if (cycle.kind === 'wake_cycle') {
            expect(cycle.minutes).toBe(20);
            expect(cycle.reason).toBe('quiet');
            expect(cycle.id).toBe('s');
        }
    });

    it('finds the reload boundary between a historical turn and a live one', () => {
        const hist = {...asst('h', [sleep('c')]), historical: true} as unknown as Turn;
        const live = asst('l', [sleep('d')]);
        expect(isSessionBoundary(hist, live)).toBe(true);
        expect(isSessionBoundary(live, hist)).toBe(false);
        expect(isSessionBoundary(hist, undefined)).toBe(false);
        expect(isSessionBoundary(sys('s', '[SITREP] x'), live)).toBe(false);
    });

    it('exposes the outer turns of a view so dividers land between views', () => {
        const [cycle] = foldWakeCycles([sys('s', '[SITREP] x'), asst('a', [sleep('c')])]);
        expect(lastTurnOf(cycle).id).toBe('a');
        const [plain] = foldWakeCycles([sys('r', 'Session resumed')]);
        expect(lastTurnOf(plain).id).toBe('r');
    });

    it('a wake that errored, was interrupted, or whose sleep failed is not quiet', () => {
        expect(isQuietWakeTurn(asst('a', [sleep('c')], 'error'))).toBe(false);
        expect(isQuietWakeTurn(asst('a', [sleep('c')], 'interrupted'))).toBe(false);
        const failedSleep = {...(call('c', SLEEP_TOOL, {minutes: 5}) as object), status: 'error'} as unknown as TurnEvent;
        expect(isQuietWakeTurn(asst('a', [failedSleep]))).toBe(false);
    });

    it('finds the next speaker past sitreps and compaction rows', () => {
        const histWake = {...(asst('h', [sleep('c')]) as object), historical: true} as unknown as Turn;
        const views = foldWakeCycles([histWake, sys('s', '[SITREP] x'), asst('l', [sleep('d')])]);
        expect(nextSpeakingTurn(views, 0)?.id).toBe('l');
        expect(nextSpeakingTurn(views, 1)).toBeUndefined();
        const marker = foldWakeCycles([histWake, sys('r', 'Session resumed'), histWake]);
        expect(nextSpeakingTurn(marker, 0)?.id).toBe('h');
    });

    it('a history sitrep between two history wakes is not a reload boundary', () => {
        const histWake = (id: string) =>
            ({...(asst(id, [sleep('c-' + id)]) as object), historical: true} as unknown as Turn);
        const allHistory = foldWakeCycles([histWake('h1'), sys('s', '[SITREP] x'), histWake('h2')]);
        expect(isSessionBoundary(lastTurnOf(allHistory[0]), nextSpeakingTurn(allHistory, 0))).toBe(false);
        const thenLive = foldWakeCycles([histWake('h1'), sys('s', '[SITREP] x'), asst('l', [sleep('c')])]);
        expect(isSessionBoundary(lastTurnOf(thenLive[0]), nextSpeakingTurn(thenLive, 0))).toBe(true);
    });
});
