import {
    ChangeDetectionStrategy,
    Component,
    DestroyRef,
    computed,
    effect,
    inject,
    input,
    signal,
    untracked,
} from '@angular/core';
import {RouterLink} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {JobSubagent} from '../../core/models/api.model';
import {
    DELEGATE_STATUS_LABEL_KEYS,
    DelegateBatchMember,
    DelegateMemberStatus,
    delegateMemberStatus,
    delegateMemberTone,
    needsChildPolling,
    summarizeDelegateBatch,
} from '../../core/models/delegate-batch.model';
import {SubagentWatchService} from '../../core/services/subagent-watch.service';
import {formatTokens} from '../../core/util/format-tokens';
import {AppBadgeComponent, BadgeTone} from '../badge';
import {AppIconComponent} from '../icon';
import {AppToolCardComponent} from './tool-card.component';

interface DelegateBatchRow {
    readonly member: DelegateBatchMember;
    /** The child's roster row, joined by `parent_tool_call_id`; null if none yet. */
    readonly child: JobSubagent | null;
    readonly status: DelegateMemberStatus;
    readonly tone: BadgeTone;
    readonly labelKey: string;
    /** Compact token count for the metrics line ('' without a child). */
    readonly tokens: string;
}

/**
 * A subagent fan-out: the `delegate_agent` calls of one assistant message,
 * rendered as one card with a row per child instead of N stacked cards.
 *
 * The sibling of `JobBatchCardComponent`, with the same header, the same
 * default-open rule and the same "finished, not done" count. That component
 * cannot be reused: it is wired to job state and job actions. Here a row is the
 * call's own facts (type, brief, status) joined to the child's roster row by
 * `parent_tool_call_id` — handle, lifecycle status, turns, tokens and a link to
 * the child transcript — from `SubagentWatchService`.
 *
 * Why the join matters while a batch runs: the session reports all results of
 * a batch at once, when the slowest child ends, and announces every call as
 * started even while it waits behind the concurrency cap. Without the roster
 * every row reads "running" until the end; with it each row says queued,
 * running or finished as the child does.
 *
 * Each row expands to the ordinary tool card for the full brief and report.
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §6.5.
 */
@Component({
    selector: 'app-delegate-batch-card',
    standalone: true,
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [TranslocoPipe, RouterLink, AppBadgeComponent, AppIconComponent, AppToolCardComponent],
    template: `
    <div class="db">
      <button type="button" class="db__head" [attr.aria-expanded]="open()"
              (click)="open.set(!open())">
        <app-icon size="sm" class="db__chevron">{{ open() ? 'expand_more' : 'chevron_right' }}</app-icon>
        <app-icon size="sm" class="db__icon">group_work</app-icon>
        <span class="db__title">{{ 'toolCard.delegateBatch.title' | transloco:{count: summary().total} }}</span>
        <span class="db__meta">{{ 'toolCard.delegateBatch.finished' | transloco:{done: summary().finished, total: summary().total} }}</span>
        @if (summary().failed; as n) {
          <span class="db__chip db__failedChip">{{ 'toolCard.delegateBatch.failed' | transloco:{count: n} }}</span>
        }
      </button>

      @if (open()) {
        <div class="db__rows">
          @for (r of rows(); track r.member.callId) {
            <div class="db__row">
              <button type="button" class="db__rowHead"
                      [attr.aria-expanded]="isExpanded(r.member.callId)"
                      (click)="toggleRow(r.member.callId)">
                <app-icon size="xs" class="db__chevron">{{ isExpanded(r.member.callId) ? 'expand_more' : 'chevron_right' }}</app-icon>
                @if (r.member.subagentType) {
                  <span class="db__type" [title]="r.member.subagentType">{{ r.member.subagentType }}</span>
                }
                <span class="db__desc" [title]="r.member.description">{{ r.member.description }}</span>
                <app-badge class="db__status" size="xs" [tone]="r.tone">{{ r.labelKey | transloco }}</app-badge>
              </button>
              @if (r.child; as c) {
                <div class="db__facts">
                  <code class="db__handle">{{ c.handle }}</code>
                  <span class="db__metrics">{{ 'jobs.detail.subagentsMetrics' | transloco:{turns: c.turns, tokens: r.tokens} }}</span>
                  <a class="db__link" [routerLink]="['/sessions', c.thread_id]">{{ 'jobs.detail.subagentsTranscript' | transloco }}</a>
                </div>
              }
              @if (isExpanded(r.member.callId)) {
                <div class="db__body">
                  <app-tool-card [view]="r.member.view" [defaultOpen]="true" />
                </div>
              }
            </div>
          }
        </div>
      }
    </div>
  `,
    styles: `
    :host { display: block; }
    .db {
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface); overflow: hidden;
    }
    .db__head {
      display: flex; align-items: center; gap: 6px; width: 100%;
      padding: 6px 8px; background: transparent; border: 0; color: inherit;
      font: inherit; font-size: 12px; text-align: left; cursor: pointer;
    }
    .db__head:hover, .db__rowHead:hover { background: var(--hover); }
    .db__chevron, .db__icon { opacity: 0.7; flex: none; }
    .db__title { font-weight: 600; }
    .db__meta { color: var(--text-secondary); font-size: 11.5px; }
    /* Same tinted pill as the job batch: bare --danger text is too faint to
       carry the one signal in the header that means "something went wrong". */
    .db__chip {
      font-size: 11px; line-height: 1.5; padding: 0 6px;
      border-radius: var(--radius-pill); white-space: nowrap;
    }
    .db__failedChip { background: var(--danger-tint); color: var(--danger); }
    .db__rows { display: flex; flex-direction: column; }
    .db__row { border-top: 1px solid var(--border-color); }
    /* One line per child: type, brief, status. The brief takes what is left
       and ellipsizes; type and status never wrap, so a 375 px screen keeps
       all three readable. */
    .db__rowHead {
      display: flex; align-items: center; gap: 6px; width: 100%; min-width: 0;
      padding: 6px 8px 2px; background: transparent; border: 0; color: inherit;
      font: inherit; font-size: 12px; line-height: 1.4; text-align: left; cursor: pointer;
    }
    .db__type {
      flex: none; max-width: 40%;
      font-family: var(--font-mono); font-size: 11px; color: var(--text-secondary);
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .db__desc {
      flex: 1 1 auto; min-width: 0;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .db__status { flex: none; }
    .db__facts {
      display: flex; flex-wrap: wrap; align-items: baseline; gap: 2px 10px;
      padding: 0 8px 6px 28px;
      font-size: 11px; color: var(--text-secondary);
    }
    .db__handle { font-family: var(--font-mono); font-size: 11px; }
    .db__link { color: var(--accent-color); }
    .db__body { padding: 0 8px 8px 28px; }
    @media (max-width: 480px) {
      .db__facts, .db__body { padding-left: 8px; }
    }
  `,
})
export class DelegateBatchCardComponent {
    /** One member per `delegate_agent` call of the message, in provider order. */
    readonly members = input.required<DelegateBatchMember[]>();
    /** The session that issued the calls; the roster is read for it. */
    readonly parentThreadId = input<string | null>(null);
    /**
     * Assistant prose follows this fan-out in the transcript. A call still
     * without a result is then a crash state a recovery settles, not a running
     * child: rows show the child's last status and nothing is polled.
     */
    readonly superseded = input<boolean>(false);

    private readonly watcher = inject(SubagentWatchService);

    /** Default open and never collapsing itself, for the job batch's reason. */
    protected readonly open = signal(true);
    private readonly expanded = signal<ReadonlySet<string>>(new Set());

    protected readonly rows = computed<DelegateBatchRow[]>(() => {
        const roster = this.watcher.roster(this.parentThreadId());
        return this.members().map((member) => {
            const child = roster?.byCall.get(member.callId) ?? null;
            const status = delegateMemberStatus(
                member,
                child,
                roster?.requestedAt ?? null,
                this.superseded(),
            );
            return {
                member,
                child,
                status,
                tone: delegateMemberTone(status),
                labelKey: DELEGATE_STATUS_LABEL_KEYS[status],
                tokens: child ? formatTokens(child.tokens) : '',
            };
        });
    });

    protected readonly summary = computed(() =>
        summarizeDelegateBatch(this.rows().map((r) => r.status)),
    );

    /**
     * A child is queued or running: the roster is worth polling. Never for a
     * superseded batch — a child row a crash left `running` would poll forever.
     */
    private readonly live = computed(
        () => !this.superseded() && this.rows().some((r) => needsChildPolling(r.status)),
    );

    constructor() {
        effect(() => {
            const threadId = this.parentThreadId();
            const live = this.live();
            untracked(() => this.watcher.watch(this, threadId, live));
        });
        inject(DestroyRef).onDestroy(() => this.watcher.release(this));
    }

    protected isExpanded(callId: string): boolean {
        return this.expanded().has(callId);
    }

    protected toggleRow(callId: string): void {
        this.expanded.update((set) => {
            const next = new Set(set);
            if (!next.delete(callId)) next.add(callId);
            return next;
        });
    }
}
