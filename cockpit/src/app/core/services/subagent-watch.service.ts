import {DOCUMENT} from '@angular/common';
import {DestroyRef, Injectable, computed, inject, signal} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {Subscription, timer} from 'rxjs';
import {exhaustMap, filter, map} from 'rxjs/operators';
import {JobSubagent} from '../models/api.model';
import {SubagentRosterSnapshot} from '../models/delegate-batch.model';
import {ApiService} from './api.service';

/**
 * Live child rows for the subagent fan-out cards of a session transcript.
 *
 * One roster request per parent session serves every card in it: the endpoint
 * (`GET /api/persistent/threads/{id}/subagents`) returns all of that session's
 * children, and cards look their rows up by the `delegate_agent` call id
 * (`parent_tool_call_id`). Rows live here, in a signal map, rather than on the
 * card's members — members are memoized per group object, the same reason
 * `JobWatchService` gives for keeping job state off `ToolCardView`.
 *
 * **When it polls.** A card declares its demand with {@link watch}: the parent
 * session, and whether any of its rows can still change (a child queued or
 * running). A session is polled while at least one card needs it live, only
 * while the tab is visible, and never after the last card releases it. A card
 * with nothing live still gets one read, shared by every card of the session,
 * so a finished batch in history can show handles and transcript links; after
 * that read (answered or failed) it costs nothing.
 *
 * **Why polling.** Children have no event journal and emit no progress frame;
 * the child transcript page polls the same durable rows for the same reason
 * (`SUBAGENT_REFRESH_MS` in `chat-page.component.ts`), at the same rate.
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §6.5 step 2.
 */

/** Poll cadence while a child is live. Matches the child transcript page. */
export const SUBAGENT_POLL_MS = 5_000;

interface Demand {
    readonly threadId: string;
    readonly live: boolean;
}

@Injectable({providedIn: 'root'})
export class SubagentWatchService {
    private readonly api = inject(ApiService);
    private readonly document = inject(DOCUMENT);
    private readonly destroyRef = inject(DestroyRef);

    /** parent thread id -> (parent_tool_call_id -> child row). */
    private readonly rostersByThread = signal<ReadonlyMap<string, SubagentRosterSnapshot>>(new Map());

    /** Signal-friendly accessor for a card's `computed()`s. */
    readonly snapshot = computed(() => this.rostersByThread());

    /** What each consumer (a card instance) currently needs. */
    private readonly demands = new Map<object, Demand>();

    /** One poller per parent session. */
    private readonly pollers = new Map<string, Subscription>();

    /**
     * Sessions that got an answer, good or failed, since a card last started
     * needing them. A card with nothing live wants one read; a failure counts,
     * so a read that can never succeed (the viewer is not the owner) is not
     * retried for a finished batch. Forgotten once no card needs the session,
     * so coming back to it reads again rather than showing a stale roster.
     */
    private readonly answered = new Set<string>();

    /**
     * Declare what `consumer` needs: rows of `parentThreadId`, and whether one
     * of them is still live. Idempotent; call it again whenever either
     * changes. A null thread withdraws the demand.
     */
    watch(consumer: object, parentThreadId: string | null, live: boolean): void {
        const previous = this.demands.get(consumer);
        if (parentThreadId) this.demands.set(consumer, {threadId: parentThreadId, live});
        else this.demands.delete(consumer);
        // A card that just went quiet gets one more read. The batch's results
        // land in one burst when the slowest child ends, and the last poll ran
        // up to one interval before that: without this read the slowest row
        // keeps its stale turns/tokens and says "completed" for a child that
        // ended `capped` or `interrupted`.
        if (previous?.live && !live && previous.threadId === parentThreadId) {
            this.answered.delete(previous.threadId);
        }
        this.reconcile();
    }

    /** Withdraw `consumer`'s demand. Call on destroy. */
    release(consumer: object): void {
        this.demands.delete(consumer);
        this.reconcile();
    }

    /** The last read of one session's children, or null until one lands. */
    roster(parentThreadId: string | null): SubagentRosterSnapshot | null {
        if (!parentThreadId) return null;
        return this.rostersByThread().get(parentThreadId) ?? null;
    }

    /** True while a request loop is running for this session. For specs. */
    isPolling(parentThreadId: string): boolean {
        return this.pollers.has(parentThreadId);
    }

    private reconcile(): void {
        const wanted = new Set<string>();
        const demanded = new Set<string>();
        for (const {threadId, live} of this.demands.values()) {
            demanded.add(threadId);
            if (live || !this.answered.has(threadId)) wanted.add(threadId);
        }
        for (const threadId of this.answered) {
            if (!demanded.has(threadId)) this.answered.delete(threadId);
        }
        for (const [threadId, poller] of this.pollers) {
            if (wanted.has(threadId)) continue;
            poller.unsubscribe();
            this.pollers.delete(threadId);
        }
        for (const threadId of wanted) {
            if (!this.pollers.has(threadId)) this.pollers.set(threadId, this.poll(threadId));
        }
    }

    private poll(threadId: string): Subscription {
        return timer(0, SUBAGENT_POLL_MS)
            .pipe(
                // A hidden tab skips its ticks; the next visible tick reads.
                filter(() => this.document.visibilityState !== 'hidden'),
                // A slow response is not stacked behind or cancelled by the
                // next tick; that tick is simply dropped.
                exhaustMap(() => {
                    const requestedAt = Date.now();
                    return this.api
                        .getSessionSubagents(threadId)
                        .pipe(map((roster) => ({roster, requestedAt})));
                }),
                takeUntilDestroyed(this.destroyRef),
            )
            .subscribe(({roster, requestedAt}) => {
                this.answered.add(threadId);
                // A failed read keeps the rows already shown.
                if (roster) {
                    const byCall = new Map<string, JobSubagent>();
                    for (const row of roster.subagents ?? []) {
                        if (row.parent_tool_call_id) byCall.set(row.parent_tool_call_id, row);
                    }
                    this.rostersByThread.update((m) => new Map(m).set(threadId, {byCall, requestedAt}));
                }
                // The one read for a consumer with nothing live ends here.
                this.reconcile();
            });
    }
}
