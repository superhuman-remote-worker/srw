import {TestBed} from '@angular/core/testing';
import {Observable, of} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {JobSubagent, ThreadSubagentRoster} from '../models/api.model';
import {ApiService} from './api.service';
import {SUBAGENT_POLL_MS, SubagentWatchService} from './subagent-watch.service';

/**
 * The poller behind the subagent fan-out card. What matters: it reads only
 * while a card needs it and the tab is visible, one request per session no
 * matter how many cards, and it stops when nothing live is left or the last
 * card goes away.
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §6.5 step 2.
 */

function row(callId: string | null, status: JobSubagent['status'] = 'running'): JobSubagent {
    return {
        thread_id: `child-${callId}`,
        handle: `explorer-${callId}`,
        subagent_type: 'explorer',
        status,
        parent_tool_call_id: callId,
        turns: 3,
        tokens: 1200,
    } as JobSubagent;
}

function roster(threadId: string, rows: JobSubagent[]): ThreadSubagentRoster {
    return {parent_thread_id: threadId, count: rows.length, subagents: rows};
}

describe('SubagentWatchService', () => {
    let service: SubagentWatchService;
    let respond: (threadId: string) => Observable<ThreadSubagentRoster | null>;
    let get: ReturnType<typeof vi.fn>;
    let visibility: DocumentVisibilityState;
    const card = (name: string) => ({name});

    beforeEach(() => {
        vi.useFakeTimers();
        visibility = 'visible';
        Object.defineProperty(document, 'visibilityState', {configurable: true, get: () => visibility});
        respond = (threadId) => of(roster(threadId, [row('c1')]));
        get = vi.fn((threadId: string) => respond(threadId));
        TestBed.configureTestingModule({
            providers: [{provide: ApiService, useValue: {getSessionSubagents: get}}],
        });
        service = TestBed.inject(SubagentWatchService);
    });

    afterEach(() => {
        TestBed.resetTestingModule();
        vi.useRealTimers();
        // Drop the instance override; the prototype getter is jsdom's own.
        delete (document as unknown as {visibilityState?: unknown}).visibilityState;
    });

    const tick = (ms: number) => vi.advanceTimersByTime(ms);

    it('reads at once and then every interval while a card has a live child', () => {
        service.watch(card('a'), 'parent', true);
        tick(0);
        expect(get).toHaveBeenCalledTimes(1);
        expect(get).toHaveBeenCalledWith('parent');
        tick(SUBAGENT_POLL_MS);
        tick(SUBAGENT_POLL_MS);
        expect(get).toHaveBeenCalledTimes(3);
    });

    it('joins rows by parent_tool_call_id and skips rows without one', () => {
        respond = (t) => of(roster(t, [row('c1', 'completed'), row(null), row('c2', 'queued')]));
        service.watch(card('a'), 'parent', true);
        tick(0);
        const snapshot = service.roster('parent');
        expect([...snapshot!.byCall.keys()]).toEqual(['c1', 'c2']);
        expect(snapshot!.byCall.get('c1')?.status).toBe('completed');
        expect(snapshot!.requestedAt).toBe(Date.now());
    });

    it('reads once more after the card reports nothing live, then stops', () => {
        // The results of a batch land when the slowest child ends; the last
        // live poll ran up to one interval earlier. The final read picks up
        // that child's terminal status (capped, interrupted) and counters.
        const a = card('a');
        service.watch(a, 'parent', true);
        tick(0);
        expect(get).toHaveBeenCalledTimes(1);
        respond = (t) => of(roster(t, [row('c1', 'capped')]));
        service.watch(a, 'parent', false);
        tick(SUBAGENT_POLL_MS);
        expect(get).toHaveBeenCalledTimes(2);
        expect(service.roster('parent')?.byCall.get('c1')?.status).toBe('capped');
        tick(SUBAGENT_POLL_MS * 5);
        expect(get).toHaveBeenCalledTimes(2);
        expect(service.isPolling('parent')).toBe(false);
    });

    it('gives a card with nothing live one read, then stops', () => {
        // A finished batch in history still gets handles and transcript links.
        service.watch(card('a'), 'parent', false);
        tick(0);
        expect(service.roster('parent')).not.toBeNull();
        tick(SUBAGENT_POLL_MS * 5);
        expect(get).toHaveBeenCalledTimes(1);
        expect(service.isPolling('parent')).toBe(false);
    });

    it('stops when the last card is released (destroyed)', () => {
        const a = card('a');
        service.watch(a, 'parent', true);
        tick(0);
        service.release(a);
        tick(SUBAGENT_POLL_MS * 5);
        expect(get).toHaveBeenCalledTimes(1);
        expect(service.isPolling('parent')).toBe(false);
    });

    it('keeps polling for the cards that still need it', () => {
        const a = card('a');
        const b = card('b');
        service.watch(a, 'parent', true);
        service.watch(b, 'parent', true);
        tick(0);
        // One request per session, however many cards read it.
        expect(get).toHaveBeenCalledTimes(1);
        service.release(a);
        tick(SUBAGENT_POLL_MS);
        expect(get).toHaveBeenCalledTimes(2);
        service.release(b);
        tick(SUBAGENT_POLL_MS * 3);
        expect(get).toHaveBeenCalledTimes(2);
    });

    it('withdraws the demand when the card loses its thread', () => {
        const a = card('a');
        service.watch(a, 'parent', true);
        tick(0);
        service.watch(a, null, true);
        tick(SUBAGENT_POLL_MS * 3);
        expect(get).toHaveBeenCalledTimes(1);
    });

    it('reads nothing while the tab is hidden, and resumes when it is visible', () => {
        visibility = 'hidden';
        service.watch(card('a'), 'parent', true);
        tick(0);
        tick(SUBAGENT_POLL_MS * 3);
        expect(get).not.toHaveBeenCalled();
        visibility = 'visible';
        tick(SUBAGENT_POLL_MS);
        expect(get).toHaveBeenCalledTimes(1);
    });

    it('keeps the rows it has when a read fails, and keeps trying', () => {
        service.watch(card('a'), 'parent', true);
        tick(0);
        respond = () => of(null);
        tick(SUBAGENT_POLL_MS);
        expect(get).toHaveBeenCalledTimes(2);
        expect(service.roster('parent')?.byCall.has('c1')).toBe(true);
        tick(SUBAGENT_POLL_MS);
        expect(get).toHaveBeenCalledTimes(3);
    });

    it('does not retry the one read of a finished batch when it fails', () => {
        // A read that can never succeed (the viewer is not the owner) must
        // not become a request every five seconds for a batch that is over.
        respond = () => of(null);
        service.watch(card('a'), 'parent', false);
        tick(0);
        tick(SUBAGENT_POLL_MS * 5);
        expect(get).toHaveBeenCalledTimes(1);
        expect(service.roster('parent')).toBeNull();
        expect(service.isPolling('parent')).toBe(false);
    });

    it('reads once more when a card comes back to the session later', () => {
        // Leaving the session destroys its cards; a batch may have run
        // elsewhere meanwhile, so the next visit must not trust the old read.
        const a = card('a');
        service.watch(a, 'parent', false);
        tick(0);
        service.release(a);
        service.watch(card('b'), 'parent', false);
        tick(0);
        expect(get).toHaveBeenCalledTimes(2);
        tick(SUBAGENT_POLL_MS * 3);
        expect(get).toHaveBeenCalledTimes(2);
    });

    it('polls each session on its own', () => {
        service.watch(card('a'), 'one', true);
        service.watch(card('b'), 'two', false);
        tick(0);
        expect(get.mock.calls.map(([t]) => t).sort()).toEqual(['one', 'two']);
        tick(SUBAGENT_POLL_MS);
        // 'two' had its one read; only 'one' is still live.
        expect(get.mock.calls.map(([t]) => t).filter((t) => t === 'two')).toHaveLength(1);
        expect(get.mock.calls.map(([t]) => t).filter((t) => t === 'one')).toHaveLength(2);
    });

    it('stops every poller when the app is torn down', () => {
        service.watch(card('a'), 'parent', true);
        tick(0);
        TestBed.resetTestingModule();
        tick(SUBAGENT_POLL_MS * 3);
        expect(get).toHaveBeenCalledTimes(1);
    });
});
