import {computed, signal, ɵresolveComponentResources} from '@angular/core';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {provideRouter} from '@angular/router';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {afterEach, beforeAll, describe, expect, it, vi} from 'vitest';
import {JobSubagent} from '../../core/models/api.model';
import {
    buildDelegateBatchMembers,
    DelegateBatchMember,
    QUEUED_AFTER_MS,
    SubagentRosterSnapshot,
} from '../../core/models/delegate-batch.model';
import {ToolCardStatus, ToolCardView} from '../../core/models/tool-card.model';
import {AssistantTurn, groupEvents, isAssistantTurn, ToolCallEvent} from '../../core/models/turn.model';
import {historyToTurns} from '../../core/services/persistent-chat.service';
import {SubagentWatchService} from '../../core/services/subagent-watch.service';
import {toolCardViewFromEvent} from '../../core/tools/tool-card-adapters';
import {DelegateBatchCardComponent} from './delegate-batch-card.component';

/**
 * The subagent fan-out card. What matters: one row per call with type, brief
 * and status; each row joined to its child by `parent_tool_call_id`; a header
 * that states the outcome; and a poll demand that follows the rows and ends
 * with the card.
 *
 * The row body is stubbed: `<app-tool-card>` has a required signal input this
 * pipeline cannot bind (see the stub's docstring).
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §6.5.
 */
vi.mock('./tool-card.component', () => import('./tool-card.stub'));

const T0 = 1_000_000;

function member(
    callId: string,
    type: string,
    brief: string,
    callStatus: ToolCardStatus = 'running',
    answered = callStatus === 'ok' || callStatus === 'error',
): DelegateBatchMember {
    return {
        callId,
        subagentType: type,
        description: brief,
        callStatus,
        answered,
        startedAt: T0,
        view: {
            tool: 'delegate_agent',
            title: 'Delegate to subagent',
            icon: 'group_work',
            subtitle: `${type}: ${brief}`,
            status: callStatus,
            params: [],
            details: [],
        } as ToolCardView,
    };
}

function child(callId: string, status: JobSubagent['status'], over: Partial<JobSubagent> = {}): JobSubagent {
    return {
        thread_id: `child-${callId}`,
        handle: `h-${callId}`,
        subagent_type: 'explorer',
        status,
        parent_tool_call_id: callId,
        turns: 4,
        tokens: 12_000,
        ...over,
    } as JobSubagent;
}

class FakeWatcher {
    private readonly rosters = signal(new Map<string, SubagentRosterSnapshot>());
    readonly snapshot = computed(() => this.rosters());
    readonly watch = vi.fn();
    readonly release = vi.fn();
    roster(threadId: string | null): SubagentRosterSnapshot | null {
        return threadId ? (this.rosters().get(threadId) ?? null) : null;
    }
    put(threadId: string, rows: JobSubagent[], requestedAt = T0 + QUEUED_AFTER_MS): void {
        const byCall = new Map(rows.map((r) => [r.parent_tool_call_id!, r] as const));
        this.rosters.update((m) => new Map(m).set(threadId, {byCall, requestedAt}));
    }
}

describe('DelegateBatchCardComponent', () => {
    let watcher: FakeWatcher;
    let fixture: ComponentFixture<DelegateBatchCardComponent>;

    beforeAll(async () => {
        await ɵresolveComponentResources(() => Promise.resolve(''));
    });

    afterEach(() => TestBed.resetTestingModule());

    async function settle() {
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
    }

    async function render(
        members: DelegateBatchMember[],
        parentThreadId: string | null = 'parent',
        superseded = false,
    ) {
        watcher = new FakeWatcher();
        TestBed.configureTestingModule({
            imports: [
                DelegateBatchCardComponent,
                TranslocoTestingModule.forRoot({
                    langs: {
                        en: {
                            toolCard: {
                                delegateBatch: {
                                    title: '{{count}} subagents',
                                    finished: '{{done}}/{{total}} finished',
                                    failed: '{{count}} failed',
                                    interrupted: '{{count}} interrupted',
                                    notStarted: '{{count}} not started',
                                    status: {
                                        pending: 'Awaiting approval',
                                        denied: 'Denied',
                                        expired: 'Not run',
                                        notStarted: 'Not started',
                                    },
                                },
                            },
                            jobs: {
                                detail: {
                                    subagentsMetrics: '{{turns}} turns · {{tokens}} tokens',
                                    subagentsTranscript: 'Transcript',
                                    subagentsStatuses: {
                                        queued: 'Queued', running: 'Running', completed: 'Completed',
                                        parked: 'Parked', interrupted: 'Interrupted', capped: 'Capped',
                                        error: 'Error', cancelled: 'Cancelled',
                                    },
                                },
                            },
                        },
                    },
                    translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
                }),
            ],
            providers: [provideRouter([]), {provide: SubagentWatchService, useValue: watcher}],
        });
        await TestBed.compileComponents();
        fixture = TestBed.createComponent(DelegateBatchCardComponent);
        // Assigned, not setInput — this pipeline drops signal-input metadata.
        const instance = fixture.componentInstance as unknown as {
            members: () => DelegateBatchMember[];
            parentThreadId: () => string | null;
            superseded: () => boolean;
        };
        instance.members = signal(members);
        instance.parentThreadId = signal(parentThreadId);
        instance.superseded = signal(superseded);
        await settle();
    }

    const root = () => fixture.nativeElement as HTMLElement;
    const text = (sel: string, from: ParentNode = root()) => from.querySelector(sel)?.textContent?.trim();
    const rows = () => [...root().querySelectorAll<HTMLElement>('.db__row')];
    const statusOf = (r: HTMLElement) => text('.db__status', r);

    const three = () => [
        member('c1', 'explorer', 'Map the auth flow'),
        member('c2', 'reviewer', 'Review the diff'),
        member('c3', 'probe', 'Check the cache'),
    ];

    it('renders one row per call with type, brief and status', async () => {
        await render(three());
        expect(text('.db__title')).toBe('3 subagents');
        expect(rows()).toHaveLength(3);
        const [first] = rows();
        expect(text('.db__type', first)).toBe('explorer');
        expect(text('.db__desc', first)).toBe('Map the auth flow');
        // Before the roster lands the call's own state is all there is.
        expect(statusOf(first)).toBe('Running');
        expect(text('.db__meta')).toBe('0/3 finished');
        expect(root().querySelector('.db__failedChip')).toBeNull();
    });

    it('joins each row to its child by parent_tool_call_id', async () => {
        await render(three());
        watcher.put('parent', [child('c1', 'completed'), child('c2', 'running', {turns: 9, tokens: 40_000})]);
        await settle();

        const [c1, c2, c3] = rows();
        expect(statusOf(c1)).toBe('Completed');
        expect(statusOf(c2)).toBe('Running');
        // In flight with no child row: queued behind the cap.
        expect(statusOf(c3)).toBe('Queued');

        expect(text('.db__handle', c2)).toBe('h-c2');
        expect(text('.db__metrics', c2)).toBe('9 turns · 40k tokens');
        expect(c2.querySelector('a.db__link')?.getAttribute('href')).toBe('/sessions/child-c2');
        // No child, no facts line to link from.
        expect(c3.querySelector('.db__facts')).toBeNull();

        expect(text('.db__meta')).toBe('1/3 finished');
    });

    it('names failures separately from the finished count', async () => {
        await render([
            member('c1', 'explorer', 'a', 'ok'),
            member('c2', 'explorer', 'b', 'error'),
            member('c3', 'explorer', 'c', 'denied'),
        ]);
        expect(text('.db__meta')).toBe('3/3 finished');
        expect(text('.db__failedChip')).toBe('2 failed');
        expect(statusOf(rows()[2])).toBe('Denied');
    });

    it('asks for polling while a child is live, and stops asking when none is', async () => {
        await render(three());
        const card = fixture.componentInstance;
        expect(watcher.watch).toHaveBeenLastCalledWith(card, 'parent', true);

        watcher.put('parent', ['c1', 'c2', 'c3'].map((id) => child(id, 'completed')));
        await settle();
        expect(watcher.watch).toHaveBeenLastCalledWith(card, 'parent', false);
    });

    it('asks for no polling for a finished batch loaded from history', async () => {
        await render([member('c1', 'explorer', 'a', 'ok'), member('c2', 'explorer', 'b', 'ok')]);
        expect(watcher.watch).toHaveBeenLastCalledWith(fixture.componentInstance, 'parent', false);
    });

    it('does not poll for a call still waiting on approval', async () => {
        await render([member('c1', 'explorer', 'a', 'pending'), member('c2', 'explorer', 'b', 'pending')]);
        expect(watcher.watch).toHaveBeenLastCalledWith(fixture.componentInstance, 'parent', false);
        expect(statusOf(rows()[0])).toBe('Awaiting approval');
    });

    describe('calls REST history shows without results', () => {
        // The parent AI row is persisted before any child starts: after a
        // reload mid-fan-out history holds the calls, marked completed, with
        // no results behind them.
        const reloaded = () => [
            member('c1', 'explorer', 'a', 'ok', false),
            member('c2', 'explorer', 'b', 'ok', false),
        ];

        it('shows a cold reload mid-batch as running and queued, and polls', async () => {
            await render(reloaded());
            // Before the roster lands: running, not a false "2/2 finished".
            expect(text('.db__meta')).toBe('0/2 finished');
            expect(watcher.watch).toHaveBeenLastCalledWith(fixture.componentInstance, 'parent', true);

            watcher.put('parent', [child('c1', 'running')]);
            await settle();
            const [c1, c2] = rows();
            expect(statusOf(c1)).toBe('Running');
            expect(statusOf(c2)).toBe('Queued');
            expect(text('.db__meta')).toBe('0/2 finished');
            expect(watcher.watch).toHaveBeenLastCalledWith(fixture.componentInstance, 'parent', true);
        });

        it('never polls for one left unanswered in an older turn', async () => {
            // A crash left the child row running; a later answer moved on.
            await render(reloaded(), 'parent', true);
            watcher.put('parent', [child('c1', 'running'), child('c2', 'capped')]);
            await settle();
            const [c1, c2] = rows();
            expect(statusOf(c1)).toBe('Running');
            expect(statusOf(c2)).toBe('Capped');
            for (const [, , live] of watcher.watch.mock.calls) expect(live).toBe(false);
        });

        it('leaves answered calls as they were', async () => {
            await render([member('c1', 'explorer', 'a', 'ok'), member('c2', 'explorer', 'b', 'ok')]);
            watcher.put('parent', [child('c1', 'running'), child('c2', 'completed')]);
            await settle();
            // A stale live row does not outvote a returned call.
            expect(rows().map(statusOf)).toEqual(['Completed', 'Completed']);
            expect(text('.db__meta')).toBe('2/2 finished');
            for (const [, , live] of watcher.watch.mock.calls) expect(live).toBe(false);
        });
    });

    describe('a batch a recovery settled, loaded from history (§6.5 step 3)', () => {
        // The executor died mid-batch; its successor wrote a result per open
        // call with a structured marker (metrics.subagent_recovery) and one
        // continuation. Rows as the history endpoint returns them, run through
        // the same pipeline the chat component uses.
        const call = (id: string) => ({
            name: 'delegate_agent',
            args: {subagent_type: 'explorer', description: `brief ${id}`},
            id,
        });
        const result = (callId: string, cls: string, subagentStatus: string | null) => ({
            id: `r-${callId}`, role: 'tool', content: `text for ${callId}`, tool_calls: null,
            tool_call_id: callId, turn_number: 3, created_at: '2026-09-29T10:05:00+00:00',
            metrics: {subagent_recovery: {
                version: 1, kind: 'result', class: cls, tool_call_id: callId,
                thread_id: subagentStatus ? `child-${callId}` : null, handle: null,
                subagent_type: 'explorer', subagent_status: subagentStatus,
                report_path: null, delivery_id: 'batch-delivery',
            }},
        });
        function membersFromHistory(rows: unknown[]): DelegateBatchMember[] {
            const turns = historyToTurns(rows as never);
            const turn = turns.find(isAssistantTurn) as AssistantTurn;
            const [group] = groupEvents(turn.events);
            expect(group.kind).toBe('delegate_batch');
            return buildDelegateBatchMembers(
                (group as {events: ToolCallEvent[]}).events,
                toolCardViewFromEvent,
            );
        }
        const aiRow = (calls: string[]) => ({
            id: 'm1', role: 'ai', content: null, tool_calls: calls.map(call), turn_number: 3,
            metrics: null, created_at: '2026-09-29T10:00:00+00:00',
        });
        const typed = {
            id: 'typed', role: 'human', content: 'also check the tests', tool_calls: null,
            turn_number: 4, metrics: null, created_at: '2026-09-29T10:01:00+00:00',
        };

        it('shows completed, interrupted and not started, and counts them in the header', async () => {
            await render(membersFromHistory([
                aiRow(['d1', 'd2', 'd3', 'd4']),
                // Typed during the batch: sorts between the call and the settle.
                typed,
                result('d1', 'completed', 'completed'),
                result('d2', 'interrupted', 'interrupted'),
                result('d3', 'not_started', null),
                result('d4', 'not_started', null),
            ]), 'parent', true);
            // The one read a finished batch gets: two children exist.
            watcher.put('parent', [child('d1', 'completed'), child('d2', 'interrupted')]);
            await settle();

            expect(rows().map(statusOf)).toEqual(['Completed', 'Interrupted', 'Not started', 'Not started']);
            expect(text('.db__title')).toBe('4 subagents');
            expect(text('.db__meta')).toBe('4/4 finished');
            expect(text('.db__interruptedChip')).toBe('1 interrupted');
            expect(text('.db__notStartedChip')).toBe('2 not started');
            expect(root().querySelector('.db__failedChip')).toBeNull();
            // Never a child that ran: no facts line to link from.
            expect(rows()[2].querySelector('.db__facts')).toBeNull();
            for (const [, , live] of watcher.watch.mock.calls) expect(live).toBe(false);
        });

        it('shows a declined call as denied and a retired child as cancelled', async () => {
            await render(membersFromHistory([
                aiRow(['d1', 'd2']),
                result('d1', 'declined', null),
                result('d2', 'retired', 'cancelled'),
            ]));
            expect(rows().map(statusOf)).toEqual(['Denied', 'Cancelled']);
            expect(text('.db__meta')).toBe('2/2 finished');
            expect(text('.db__failedChip')).toBe('2 failed');
            expect(root().querySelector('.db__interruptedChip')).toBeNull();
            expect(root().querySelector('.db__notStartedChip')).toBeNull();
        });
    });

    it('releases its demand when destroyed', async () => {
        await render(three());
        const card = fixture.componentInstance;
        fixture.destroy();
        expect(watcher.release).toHaveBeenCalledWith(card);
    });

    it('expands one row to its tool card', async () => {
        await render(three());
        expect(root().querySelector('app-tool-card')).toBeNull();
        const head = rows()[1].querySelector<HTMLButtonElement>('.db__rowHead')!;
        expect(head.getAttribute('aria-expanded')).toBe('false');

        head.click();
        await settle();
        const cards = root().querySelectorAll('app-tool-card');
        expect(cards).toHaveLength(1);
        expect(rows()[1].querySelector('.tc-stub')?.textContent).toBe('delegate_agent:reviewer: Review the diff');
        expect(head.getAttribute('aria-expanded')).toBe('true');

        head.click();
        await settle();
        expect(root().querySelector('app-tool-card')).toBeNull();
    });

    it('opens by default and collapses on demand — never the other way round', async () => {
        await render(three());
        const head = root().querySelector<HTMLButtonElement>('.db__head')!;
        expect(head.getAttribute('aria-expanded')).toBe('true');
        head.click();
        await settle();
        expect(rows()).toHaveLength(0);
        expect(head.getAttribute('aria-expanded')).toBe('false');
    });
});
