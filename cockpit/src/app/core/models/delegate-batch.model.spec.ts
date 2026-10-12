import {describe, expect, it} from 'vitest';
import de from '../../../assets/i18n/de-DE.json';
import en from '../../../assets/i18n/en.json';
import {JobSubagent, JobSubagentStatus} from './api.model';
import {
    buildDelegateBatchMembers,
    DELEGATE_STATUS_LABEL_KEYS,
    DelegateMemberStatus,
    delegateMemberStatus,
    delegateMemberTone,
    isLiveDelegateStatus,
    needsChildPolling,
    QUEUED_AFTER_MS,
    summarizeDelegateBatch,
} from './delegate-batch.model';
import {SubagentRecoveryClass} from './subagent-recovery.model';
import {ToolCardStatus, ToolCardView} from './tool-card.model';
import {ToolCallEvent} from './turn.model';

/**
 * The fan-out card's rules, kept out of the component so they can be read and
 * tested in one place. The status mapping is the seam step 3 of §6.5 extends.
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §6.5.
 */

const T0 = 1_000_000;

function child(status: JobSubagentStatus): JobSubagent {
    return {status} as JobSubagent;
}

/** A call as the live stream shows it: a result iff it has returned. */
function member(callStatus: ToolCardStatus, startedAt = T0) {
    return {callStatus, answered: callStatus === 'ok' || callStatus === 'error', startedAt};
}

/** A call REST history shows before its result was written (reload mid-fan-out). */
function unanswered(startedAt = T0) {
    return {callStatus: 'ok' as const, answered: false, startedAt};
}

/**
 * A call whose result a delegation-batch recovery wrote. History marks it
 * `completed` (`denied` for a declined one) with a result behind it.
 */
function recovered(cls: SubagentRecoveryClass, subagentStatus: string | null = null) {
    return {
        callStatus: (cls === 'declined' ? 'denied' : 'ok') as ToolCardStatus,
        answered: true,
        startedAt: T0,
        recovery: {class: cls, subagentStatus},
    };
}

describe('buildDelegateBatchMembers', () => {
    it('takes type and brief from the call args and status from its card', () => {
        const event: ToolCallEvent = {
            kind: 'tool_call',
            id: 'call-1',
            tool: 'delegate_agent',
            args: {subagent_type: ' explorer ', description: 'Map the auth flow', prompt: 'long'},
            status: 'running',
            startedAt: T0,
        };
        const view = {status: 'running'} as ToolCardView;
        expect(buildDelegateBatchMembers([event], () => view)).toEqual([
            {
                callId: 'call-1',
                subagentType: 'explorer',
                description: 'Map the auth flow',
                callStatus: 'running',
                answered: false,
                startedAt: T0,
                view,
            },
        ]);
    });

    it('carries a recovery marker from the event', () => {
        const event: ToolCallEvent = {
            kind: 'tool_call', id: 'call-1', tool: 'delegate_agent', args: {}, status: 'completed',
            result: '[delegate_agent: NOT STARTED]', startedAt: T0,
            recovery: {class: 'not_started', subagentStatus: null},
        };
        const [m] = buildDelegateBatchMembers([event], () => ({status: 'ok'}) as ToolCardView);
        expect(m.recovery).toEqual({class: 'not_started', subagentStatus: null});
        expect(m.answered).toBe(true);
    });

    it('counts a call answered once any result is in, even an empty one', () => {
        const base = {kind: 'tool_call', id: 'c', tool: 'delegate_agent', args: {}, startedAt: 0} as const;
        const toView = () => ({status: 'ok'}) as ToolCardView;
        const [withResult] = buildDelegateBatchMembers([{...base, status: 'completed', result: ''}], toView);
        // historyToTurns marks a call with no tool row `completed` too.
        const [withoutResult] = buildDelegateBatchMembers([{...base, status: 'completed'}], toView);
        expect(withResult.answered).toBe(true);
        expect(withoutResult.answered).toBe(false);
    });

    it('tolerates missing or non-string args', () => {
        const event = {
            kind: 'tool_call', id: 'c', tool: 'delegate_agent', args: {subagent_type: 7},
            status: 'completed', startedAt: 0,
        } as unknown as ToolCallEvent;
        const [m] = buildDelegateBatchMembers([event], () => ({status: 'ok'}) as ToolCardView);
        expect(m.subagentType).toBe('');
        expect(m.description).toBe('');
    });
});

describe('delegateMemberStatus', () => {
    it('says so when a call never reached a child', () => {
        expect(delegateMemberStatus(member('pending'), null, T0)).toBe('pending');
        expect(delegateMemberStatus(member('denied'), null, T0)).toBe('denied');
        expect(delegateMemberStatus(member('expired'), null, T0)).toBe('expired');
    });

    it('shows the call as running until the roster has been read', () => {
        expect(delegateMemberStatus(member('running'), null, null)).toBe('running');
    });

    it('calls a running call without a child row queued, once a later read confirms it', () => {
        // The runtime opens the child row when the child takes a slot, so a
        // call behind the cap has none.
        const read = T0 + QUEUED_AFTER_MS;
        expect(delegateMemberStatus(member('running'), null, read)).toBe('queued');
    });

    it('does not call a child queued on a read that raced its row insert', () => {
        // The first poll of a fresh batch goes out milliseconds after the
        // frame, before the starting child's row exists.
        expect(delegateMemberStatus(member('running'), null, T0 + 50)).toBe('running');
        // A read from before the call (a stale roster) proves nothing either.
        expect(delegateMemberStatus(member('running'), null, T0 - 60_000)).toBe('running');
    });

    it('trusts the child row while the call runs — each child ends on its own', () => {
        // The session reports a batch's results all at once (F5); the roster
        // is what shows a finished child before its slowest sibling ends.
        expect(delegateMemberStatus(member('running'), child('running'), T0)).toBe('running');
        expect(delegateMemberStatus(member('running'), child('completed'), T0)).toBe('completed');
        expect(delegateMemberStatus(member('running'), child('capped'), T0)).toBe('capped');
    });

    it('lets a terminal child row say more than "completed" once the call returned', () => {
        expect(delegateMemberStatus(member('ok'), child('capped'), T0)).toBe('capped');
        expect(delegateMemberStatus(member('ok'), child('interrupted'), T0)).toBe('interrupted');
    });

    it('does not let a stale live row outvote a returned call', () => {
        expect(delegateMemberStatus(member('ok'), child('running'), T0)).toBe('completed');
        expect(delegateMemberStatus(member('error'), child('queued'), T0)).toBe('error');
    });

    it('falls back to the call state when the call returned without a child row', () => {
        // Refused inside the tool, or history from before the roster.
        expect(delegateMemberStatus(member('ok'), null, T0)).toBe('completed');
        expect(delegateMemberStatus(member('error'), null, T0)).toBe('error');
    });

    describe('a call history shows without its result (reload mid-fan-out)', () => {
        // The parent AI row is persisted before any child starts, so REST
        // history holds the calls with no results and marks them completed.
        it('takes the child row as the truth, live or finished', () => {
            expect(delegateMemberStatus(unanswered(), child('running'), T0 + QUEUED_AFTER_MS)).toBe('running');
            expect(delegateMemberStatus(unanswered(), child('completed'), T0 + QUEUED_AFTER_MS)).toBe('completed');
        });

        it('is queued with no child row, and running until the roster is read', () => {
            expect(delegateMemberStatus(unanswered(), null, T0 + 60_000)).toBe('queued');
            expect(delegateMemberStatus(unanswered(), null, null)).toBe('running');
        });

        it('keeps the child row status but is never queued once a later answer superseded it', () => {
            // A crash state a recovery settles (WP1/WP2), not a live batch.
            expect(delegateMemberStatus(unanswered(), child('running'), T0 + 60_000, true)).toBe('running');
            expect(delegateMemberStatus(unanswered(), child('capped'), T0 + 60_000, true)).toBe('capped');
            // No child row: the call's own state until step 3 says more.
            expect(delegateMemberStatus(unanswered(), null, T0 + 60_000, true)).toBe('completed');
        });

        it('leaves an answered call exactly as before, superseded or not', () => {
            for (const superseded of [false, true]) {
                expect(delegateMemberStatus(member('ok'), child('running'), T0, superseded)).toBe('completed');
                expect(delegateMemberStatus(member('ok'), child('capped'), T0, superseded)).toBe('capped');
                expect(delegateMemberStatus(member('ok'), null, T0, superseded)).toBe('completed');
            }
        });
    });

    describe('a call whose result a recovery wrote (§6.5 step 3)', () => {
        // The executor died mid-batch; its successor wrote one result per open
        // call, each with a marker that says what became of the child.
        it('shows what the marker says, not the call\'s "completed"', () => {
            expect(delegateMemberStatus(recovered('interrupted', 'interrupted'), null, T0)).toBe('interrupted');
            expect(delegateMemberStatus(recovered('not_started'), null, T0)).toBe('not_started');
            expect(delegateMemberStatus(recovered('declined'), null, T0)).toBe('denied');
            expect(delegateMemberStatus(recovered('retired', 'cancelled'), null, T0)).toBe('cancelled');
            expect(delegateMemberStatus(recovered('completed', 'completed'), null, T0)).toBe('completed');
        });

        it('decides before the child row: the settle is the later fact', () => {
            // A roster read from before the settle still has the child running.
            expect(delegateMemberStatus(recovered('interrupted'), child('running'), T0)).toBe('interrupted');
            expect(delegateMemberStatus(recovered('retired'), child('running'), T0)).toBe('cancelled');
            expect(delegateMemberStatus(recovered('not_started'), null, T0 + 60_000)).toBe('not_started');
        });

        it('decides for an abandoned call too, which without it keeps its own status', () => {
            // Step 2 left a superseded call with no child row on "completed";
            // the recovered row now says what happened.
            expect(delegateMemberStatus(unanswered(), null, T0 + 60_000, true)).toBe('completed');
            expect(delegateMemberStatus(recovered('not_started'), null, T0 + 60_000, true)).toBe('not_started');
        });

        it('lets a finished child\'s terminal status say more than "completed"', () => {
            expect(delegateMemberStatus(recovered('completed', 'capped'), null, T0)).toBe('capped');
            expect(delegateMemberStatus(recovered('completed', 'error'), null, T0)).toBe('error');
            // Nothing recorded: the child row's terminal status, else completed.
            expect(delegateMemberStatus(recovered('completed'), child('capped'), T0)).toBe('capped');
            expect(delegateMemberStatus(recovered('completed'), child('running'), T0)).toBe('completed');
            // A status this build does not know says nothing.
            expect(delegateMemberStatus(recovered('completed', 'rebooting'), null, T0)).toBe('completed');
        });
    });

    it('ignores a child status this build does not know', () => {
        const unknown = child('rebooting' as JobSubagentStatus);
        expect(delegateMemberStatus(member('running'), unknown, T0 + QUEUED_AFTER_MS)).toBe('running');
        expect(delegateMemberStatus(member('ok'), unknown, T0)).toBe('completed');
    });
});

describe('delegate batch status vocabulary', () => {
    const statuses = Object.keys(DELEGATE_STATUS_LABEL_KEYS) as DelegateMemberStatus[];

    function lookup(tree: object, key: string): unknown {
        return key.split('.').reduce<unknown>(
            (node, part) => (node && typeof node === 'object' ? (node as Record<string, unknown>)[part] : undefined),
            tree,
        );
    }

    it('has a label in both locales for every status', () => {
        // The keys are held in a map, so the static reference scan cannot
        // see them; check them here instead.
        for (const status of statuses) {
            const key = DELEGATE_STATUS_LABEL_KEYS[status];
            expect(typeof lookup(en, key), `en ${key}`).toBe('string');
            expect(typeof lookup(de, key), `de-DE ${key}`).toBe('string');
        }
    });

    it('has a tone for every status', () => {
        for (const status of statuses) expect(delegateMemberTone(status)).toBeTruthy();
    });

    it('polls only for a child that can still change on its own', () => {
        expect(statuses.filter(needsChildPolling).sort()).toEqual(['queued', 'running']);
        // A call waiting on approval is live but has no child to watch yet.
        expect(isLiveDelegateStatus('pending')).toBe(true);
        expect(needsChildPolling('pending')).toBe(false);
    });
});

describe('summarizeDelegateBatch', () => {
    it('counts finished whatever the outcome, and names failures separately', () => {
        expect(
            summarizeDelegateBatch(['completed', 'error', 'cancelled', 'denied', 'running', 'queued', 'pending']),
        ).toEqual({total: 7, finished: 4, failed: 3, interrupted: 0, notStarted: 0});
    });

    it('does not count a capped or interrupted child as failed', () => {
        // Warnings, not failures: the child stopped short, it did not error.
        expect(summarizeDelegateBatch(['capped', 'interrupted', 'not_started'])).toEqual({
            total: 3,
            finished: 3,
            failed: 0,
            interrupted: 1,
            notStarted: 1,
        });
    });

    it('counts a recovered batch\'s interrupted and never-started calls apart', () => {
        // One finished, one interrupted, two settled before they ran, one
        // declined, one retired when the session was stopped.
        expect(
            summarizeDelegateBatch(['completed', 'interrupted', 'not_started', 'not_started', 'denied', 'cancelled']),
        ).toEqual({total: 6, finished: 6, failed: 2, interrupted: 1, notStarted: 2});
    });
});
