import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  computed,
  effect,
  input,
  output,
  signal,
  viewChild,
} from '@angular/core';
import {DatePipe} from '@angular/common';
import {RouterLink} from '@angular/router';
import {TranslocoModule} from '@jsverse/transloco';
import {
  Job,
  JobProgress,
  JobSubagent,
  JobSubagentStatus,
  JobSubjob,
  JobUsage,
  WorkspaceContractProjection,
} from '../../core/models/api.model';
import {JobSummary} from '../../core/models/audit.model';
import {AppBadgeComponent, BadgeTone} from '../../ui/badge';
import {AppSpinnerComponent} from '../../ui/spinner';
import {isTerminalJobStatus, jobStatusTone} from '../../core/util/job-status';

/** Own spend, or the whole subtree beneath the job. */
export type UsageScope = 'job' | 'subtree';

/** Everything the panel loads lazily for one job, plus its load state. */
export interface JobDetailState {
  loading: boolean;
  /** Set only when every lazy call failed — a partial load still renders. */
  error: boolean;
  detail: Job | null;
  /**
   * Subtree usage, fetched only if the reader asks for it. Kept separate from
   * `usage` rather than replacing it so switching scope back and forth is free
   * and the two figures stay independently inspectable.
   */
  usageSubtree: JobUsage | null;
  loadingSubtree: boolean;
  /**
   * Whether the subtree call has been made at all. Separate from
   * `usageSubtree != null` because a failed call leaves that null forever, and
   * keying the guard off the data would refetch on every single click.
   */
  subtreeAttempted: boolean;
  usage: JobUsage | null;
  progress: JobProgress | null;
  /**
   * The job's tree, straight from the database rather than from the list query.
   *
   * Null while loading or after a failed fetch; an empty array is the positive
   * answer "this job spawned nothing". The distinction matters — the panel
   * renders nothing in both cases, but only one of them is a fact.
   */
  subjobs: JobSubjob[] | null;
  /** The job's in-process child threads. Null while loading or on failure. */
  subagents: JobSubagent[] | null;
}

/**
 * The model a job actually ran on, or null when the client cannot know it.
 *
 * `config_override.llm.model` is the only reliable source: `resolved_config`
 * comes back empty for ordinary jobs (checked against a real job on k3d), and
 * the list payload carries neither. When no override is set the model is
 * whatever the expert's config resolves to server-side, which the client cannot
 * see — so this returns null and the panel says "expert default" rather than
 * inventing a name.
 */
export function jobModelLabel(detail: Job | null): string | null {
  const raw = detail?.config_override;
  const override = typeof raw === 'string' ? safeParse(raw) : raw;
  const llm = (override as Record<string, unknown> | null)?.['llm'];
  const model = (llm as Record<string, unknown> | null)?.['model'];
  return typeof model === 'string' && model.trim() ? model.trim() : null;
}

function safeParse(value: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

/** Thousands-separated integer, or an em dash when there is nothing to show. */
export function formatCount(value: number | null | undefined): string {
  return value == null ? '—' : value.toLocaleString();
}

export function formatDurationSeconds(seconds: number | null | undefined): string | null {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return null;
  const s = Math.floor(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

/**
 * How a cost figure should read, given that "unknown" and "zero" are different
 * claims and the endpoint distinguishes four states.
 *
 * `amount` is null whenever no number may be shown; the caller renders the
 * `reasonKey` message instead. `isFloor` marks a partially-priced job, whose
 * real cost is strictly above what is displayed — rendering that bare would
 * understate it silently.
 */
export interface CostDisplay {
  amount: number | null;
  isFloor: boolean;
  reasonKey: string | null;
}

export function costDisplay(usage: JobUsage | null): CostDisplay {
  if (!usage) return {amount: null, isFloor: false, reasonKey: 'jobs.detail.costUnknown'};
  switch (usage.state) {
    case 'unavailable':
      return {amount: null, isFloor: false, reasonKey: 'jobs.detail.costMeteringOff'};
    case 'predates_ledger':
      return {amount: null, isFloor: false, reasonKey: 'jobs.detail.costPredatesLedger'};
    case 'no_usage':
      return {amount: null, isFloor: false, reasonKey: 'jobs.detail.costNoUsage'};
  }
  if (usage.cost.usd == null) {
    // Metered, but nothing carried a rate — the common case for self-hosted
    // models. Tokens are still real and are shown alongside.
    return {amount: null, isFloor: false, reasonKey: 'jobs.detail.costUnpriced'};
  }
  return {amount: usage.cost.usd, isFloor: !usage.cost.complete, reasonKey: null};
}

/**
 * Which workspace backend the job actually got, from the safe projection the
 * list row already carries. Falls back down the chain because a job that never
 * dispatched has only a requested backend.
 */
export function workspaceBackendLabel(
  contract: WorkspaceContractProjection | null | undefined,
): string | null {
  if (!contract) return null;
  const backend =
    contract.effective_backend || contract.assigned_backend || contract.requested_backend;
  return backend ? String(backend) : null;
}

/** Small costs need more than 2 decimals to avoid rendering as $0.00. */
export function formatUsd(amount: number): string {
  if (amount === 0) return '$0.00';
  if (amount < 0.01) return `$${amount.toPrecision(2)}`;
  return `$${amount.toFixed(2)}`;
}

/** Short id for the roster, which has far less room than a list row. */
export function shortJobId(id: string): string {
  return id.slice(0, 8);
}

/**
 * How long a subjob has run, or ran for, in seconds.
 *
 * A live subjob is measured against `now` — that is the number the reader
 * actually wants when the question is "is this thing progressing". Terminal
 * jobs prefer `completed_at` and fall back to `updated_at`, because
 * `completed_at` is null on rows that reached a terminal status by a path that
 * never stamped it (cancellation, older rows).
 */
export function subjobElapsedSeconds(sub: JobSubjob, now: number): number | null {
  const started = Date.parse(sub.created_at);
  if (!Number.isFinite(started)) return null;
  const endRaw = sub.completed_at ?? (isTerminalJobStatus(sub.status) ? sub.updated_at : null);
  const ended = endRaw ? Date.parse(endRaw) : now;
  if (!Number.isFinite(ended) || ended < started) return null;
  return Math.floor((ended - started) / 1000);
}

/** Elapsed time for an in-process child, using the same live/terminal shape as subjobs. */
export function subagentElapsedSeconds(sub: JobSubagent, now: number): number | null {
  const started = Date.parse(sub.started_at);
  if (!Number.isFinite(started)) return null;
  const live = sub.status === 'queued' || sub.status === 'running';
  const endRaw = sub.ended_at ?? (live ? null : sub.last_activity);
  const ended = endRaw ? Date.parse(endRaw) : now;
  if (!Number.isFinite(ended) || ended < started) return null;
  return Math.floor((ended - started) / 1000);
}

export function subagentStatusTone(status: JobSubagentStatus): BadgeTone {
  switch (status) {
    case 'completed': return 'success';
    case 'running': return 'accent';
    case 'queued': return 'info';
    case 'error':
    case 'cancelled': return 'danger';
    case 'parked':
    case 'interrupted':
    case 'capped': return 'warning';
  }
}

/** Subjobs still capable of moving on their own. */
export function liveSubjobCount(subjobs: readonly JobSubjob[]): number {
  return subjobs.filter((sub) => !isTerminalJobStatus(sub.status)).length;
}

/**
 * The sentence a parent owes its reader, or null when its status speaks for
 * itself.
 *
 * `waiting` is the one job status that is meaningless in isolation: the
 * orchestrator parks a parent there *because* a child is running
 * (`_spawn_scholar_job` holds it there while the scholar works). A reader
 * seeing `waiting` on a row with no visible children reasonably concludes the
 * job is stuck, when in fact the work is happening one level down. That is the
 * misreading this whole panel section exists to prevent.
 *
 * Returned as a key rather than a string so the caller interpolates the count
 * through transloco — and note the double braces the catalogue needs, which a
 * typecheck and a full AOT build will both happily accept if you get wrong.
 */
export function subjobBlockedKey(
  parentStatus: string | null | undefined,
  subjobs: readonly JobSubjob[],
): string | null {
  if (parentStatus !== 'waiting') return null;
  return liveSubjobCount(subjobs) > 0
    ? 'jobs.detail.waitingOnSubjobs'
    : 'jobs.detail.waitingNoLiveSubjobs';
}

@Component({
  selector: 'app-job-detail-panel',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, DatePipe, TranslocoModule, AppBadgeComponent, AppSpinnerComponent],
  template: `
    <div class="detail-panel">
      @if (job().description) {
        <div class="description-block">
          <!-- Kept on one line on purpose: the paragraph is pre-wrap, so any
               template indentation around the interpolation renders as a real
               leading space. -->
          <p
            #descBody
            class="detail-description"
            [class.detail-description--capped]="!descExpanded()"
            [class.detail-description--faded]="descOverflowed() && !descExpanded()"
          >{{ job().description }}</p>
          @if (descOverflowed()) {
            <button
              type="button"
              class="desc-toggle"
              [attr.aria-expanded]="descExpanded()"
              (click)="descExpanded.set(!descExpanded())"
            >
              {{ (descExpanded() ? 'jobs.detail.showLess' : 'jobs.detail.showMore') | transloco }}
            </button>
          }
        </div>
      }

      <div class="detail-grid">
        <div class="fact">
          <span class="fact-label">{{ 'jobs.detail.origin' | transloco }}</span>
          <span class="fact-value">
            <app-badge tone="neutral" size="xs">{{ job().origin || 'user' }}</app-badge>
          </span>
        </div>
        <div class="fact">
          <span class="fact-label">{{ 'jobs.detail.expert' | transloco }}</span>
          <span class="fact-value">{{ job().config_name || '—' }}</span>
        </div>
        <div class="fact">
          <span class="fact-label">{{ 'jobs.detail.model' | transloco }}</span>
          <span class="fact-value" [class.muted]="!modelLabel()">
            {{ modelLabel() || ('jobs.detail.modelDefault' | transloco) }}
          </span>
        </div>
        <div class="fact">
          <span class="fact-label">{{ 'jobs.detail.workspace' | transloco }}</span>
          <span class="fact-value" [class.muted]="!workspaceLabel()">
            {{ workspaceLabel() || ('jobs.detail.unknown' | transloco) }}
          </span>
        </div>
        <div class="fact">
          <span class="fact-label">{{ 'jobs.detail.duration' | transloco }}</span>
          <span class="fact-value" [class.muted]="!durationLabel()">
            {{ durationLabel() || ('jobs.detail.unknown' | transloco) }}
          </span>
        </div>
        <div class="fact">
          <span class="fact-label">{{ 'jobs.detail.auditSteps' | transloco }}</span>
          <span class="fact-value">{{ formatCount(job().audit_count) }}</span>
        </div>
      </div>

      @if (job().workspace_recovery; as recovery) {
        <section class="recovery-detail" [class.attention]="recovery.state === 'paused_attention'">
          <strong>{{ recovery.message }}</strong>
          <span>
            {{ 'jobs.recovery.deadline' | transloco }}:
            {{ recovery.deadline_at | date:'medium' }}
          </span>
          @if (recovery.next_check_at) {
            <span>
              {{ 'jobs.recovery.nextCheck' | transloco }}:
              {{ recovery.next_check_at | date:'mediumTime' }}
            </span>
          }
          @if (recovery.cleanup_pending) {
            <span>{{ 'jobs.recovery.cleanupPending' | transloco }}</span>
          }
        </section>
      }

      @if (data()?.loading || (scope() === 'subtree' && data()?.loadingSubtree)) {
        <div class="detail-loading">
          <app-spinner size="sm" />
          <span>{{ 'jobs.detail.loading' | transloco }}</span>
        </div>
      } @else {
        <div class="usage-block">
          <div class="usage-head">
            <span class="usage-title">{{ 'jobs.detail.usage' | transloco }}</span>
            <div class="scope-switch" role="group" [attr.aria-label]="'jobs.detail.scopeLabel' | transloco">
              <button
                type="button"
                [class.active]="scope() === 'job'"
                [attr.aria-pressed]="scope() === 'job'"
                (click)="setScope('job')"
              >
                {{ 'jobs.detail.scopeJob' | transloco }}
              </button>
              <button
                type="button"
                [class.active]="scope() === 'subtree'"
                [attr.aria-pressed]="scope() === 'subtree'"
                (click)="setScope('subtree')"
              >
                {{ 'jobs.detail.scopeSubtree' | transloco }}
              </button>
            </div>
            @if (activeUsage()?.freshness?.live) {
              <span class="usage-note">{{ 'jobs.detail.usageLive' | transloco }}</span>
            }
          </div>

          @if (scope() === 'subtree' && subtreeJobCount() === 1) {
            <p class="usage-reason">{{ 'jobs.detail.scopeNoSubjobs' | transloco }}</p>
          } @else if (scope() === 'subtree' && subtreeJobCount() > 1) {
            <p class="usage-reason">
              {{ 'jobs.detail.scopeCovers' | transloco: {count: subtreeJobCount()} }}
            </p>
          }

          <div class="usage-figures">
            <div class="figure">
              <span class="figure-value">{{ formatCount(activeUsage()?.llm?.total_tokens) }}</span>
              <span class="figure-label">{{ 'jobs.detail.tokens' | transloco }}</span>
            </div>
            <div class="figure">
              @if (cost().amount !== null) {
                <span class="figure-value">
                  {{ formatUsd(cost().amount!) }}
                  @if (cost().isFloor) {
                    <span class="figure-qualifier" [title]="'jobs.detail.costFloorHint' | transloco"
                      >+</span
                    >
                  }
                </span>
              } @else {
                <span class="figure-value muted">{{ 'jobs.detail.unknown' | transloco }}</span>
              }
              <span class="figure-label">{{ 'jobs.detail.cost' | transloco }}</span>
            </div>
          </div>

          @if (cost().reasonKey; as reason) {
            <p class="usage-reason">{{ reason | transloco }}</p>
          } @else if (cost().isFloor) {
            <p class="usage-reason">
              {{
                'jobs.detail.costFloor'
                  | transloco: {priced: activeUsage()!.cost.priced_events, total: activeUsage()!.cost.events}
              }}
            </p>
          }

          @if (perModel().length > 0) {
            <ul class="usage-models">
              @for (row of perModel(); track row.resource) {
                <li>
                  <span class="model-name">{{ row.resource }}</span>
                  <span class="model-tokens">{{ formatCount(row.tokens) }}</span>
                </li>
              }
            </ul>
          }
        </div>
      }

      @if (subjobs().length > 0) {
        <div class="detail-subjobs">
          <div class="subjobs-head">
            <span class="subjobs-label">
              {{ 'jobs.detail.subjobs' | transloco: {count: subjobs().length} }}
            </span>
            @if (blockedKey(); as key) {
              <span class="subjobs-blocked">
                {{ key | transloco: {count: liveCount()} }}
              </span>
            }
            @if (hiddenCount() > 0) {
              <button
                type="button"
                class="subjobs-reveal"
                (click)="revealRows.emit()"
                [title]="'jobs.detail.showSubjobRowsHint' | transloco"
              >
                {{ 'jobs.detail.showSubjobRows' | transloco: {count: hiddenCount()} }}
              </button>
            }
          </div>
          <table class="subjob-table app-table">
            <tbody>
              @for (sub of subjobs(); track sub.id) {
                <tr
                  class="subjob-row"
                  [class.subjob-live]="!isTerminal(sub.status)"
                  (click)="subjobSelected.emit(sub.id)"
                >
                  <td class="sub-role">
                    <span [style.padding-left.px]="sub.depth * 12">
                      {{ sub.config_name || ('jobs.detail.unknown' | transloco) }}
                    </span>
                  </td>
                  <td class="sub-status">
                    <app-badge [tone]="statusTone(sub.status)" size="xs">
                      {{ 'jobs.status.' + sub.status | transloco }}
                    </app-badge>
                  </td>
                  <td class="sub-desc">
                    <span class="sub-desc-text" [title]="sub.description">
                      {{ sub.description }}
                    </span>
                    @if (sub.error_message) {
                      <span class="sub-error" [title]="sub.error_message">
                        {{ sub.error_message }}
                      </span>
                    }
                  </td>
                  <td class="sub-elapsed">{{ elapsedLabel(sub) || '—' }}</td>
                  <td class="sub-id"><code>{{ shortId(sub.id) }}</code></td>
                </tr>
              }
            </tbody>
          </table>
        </div>
      }

      @if (subagents().length > 0) {
        <div class="detail-subjobs detail-subagents">
          <div class="subjobs-head">
            <span class="subjobs-label">
              {{ 'jobs.detail.subagents' | transloco: {count: subagents().length} }}
            </span>
          </div>
          <table class="subjob-table subagent-table app-table">
            <tbody>
              @for (child of subagents(); track child.thread_id) {
                <tr
                  class="subjob-row subagent-row"
                  [class.subagent-live]="child.status === 'queued' || child.status === 'running'"
                >
                  <td class="sub-role subagent-handle">{{ child.handle }}</td>
                  <td class="subagent-type">{{ child.subagent_type }}</td>
                  <td class="sub-status">
                    <app-badge [tone]="subagentTone(child.status)" size="xs">
                      {{ 'jobs.detail.subagentsStatuses.' + child.status | transloco }}
                    </app-badge>
                  </td>
                  <td class="sub-desc">
                    <span class="sub-desc-text" [title]="child.description">
                      {{ child.description }}
                    </span>
                    @if (child.outcome) {
                      <span class="sub-outcome" [title]="child.outcome">
                        {{ 'jobs.detail.subagentsOutcome' | transloco: {outcome: child.outcome} }}
                      </span>
                    }
                    @if (child.error) {
                      <span class="sub-error" [title]="child.error">
                        {{ 'jobs.detail.subagentsError' | transloco: {error: child.error} }}
                      </span>
                    }
                  </td>
                  <td class="subagent-metrics">
                    {{
                      'jobs.detail.subagentsMetrics'
                        | transloco: {turns: formatCount(child.turns), tokens: formatCount(child.tokens)}
                    }}
                  </td>
                  <td class="sub-elapsed">{{ subagentElapsedLabel(child) || '—' }}</td>
                  <td class="subagent-link">
                    <a [routerLink]="['/sessions', child.thread_id]">
                      {{ 'jobs.detail.subagentsTranscript' | transloco }}
                    </a>
                  </td>
                </tr>
              }
            </tbody>
          </table>
        </div>
      }
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
      }
      .detail-panel {
        display: flex;
        flex-direction: column;
        gap: 12px;
        padding: 12px 16px 14px;
        background: var(--surface-1);
        border-radius: var(--radius-control);
      }
      .description-block {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 6px;
        min-width: 0;
      }
      .detail-description {
        margin: 0;
        color: var(--text-primary);
        font-size: 13px;
        line-height: 1.55;
        white-space: pre-wrap;
        overflow-wrap: anywhere;
      }
      /* A job prompt is routinely hundreds of lines; left unclamped it pushes
         the facts, usage and roster below it off the screen entirely — the
         panel then reads as "a wall of text" rather than as a job summary. */
      .detail-description--capped {
        position: relative;
        /* 10 lines: em is this element's own 13px, so the cap tracks the font
           size and the 1.55 line-height rather than a hard-coded pixel guess. */
        max-height: 15.5em;
        overflow: hidden;
      }
      /* Only once something is genuinely hidden: an unfaded short prompt must
         not be dimmed by a gradient taller than the text it sits over. */
      .detail-description--faded::after {
        content: '';
        position: absolute;
        inset-inline: 0;
        bottom: 0;
        height: 3em;
        /* Fades to the panel's own background — a gradient to anything else
           reads as a smudge in whichever theme it does not match. */
        background: linear-gradient(to bottom, transparent, var(--surface-1));
        pointer-events: none;
      }
      /* Reads as a chip, not as prose: in both themes --border-color equals
         the panel background, so a bordered-but-transparent button is
         invisible at rest. A surface-0 fill on the surface-1 panel is the same
         contrast pair the scope switch below already uses. */
      .desc-toggle {
        padding: 3px 8px;
        border: none;
        border-radius: var(--radius-control);
        background: var(--surface-0);
        color: var(--text-secondary);
        font-family: inherit;
        font-size: 11px;
        cursor: pointer;
      }
      .desc-toggle:hover {
        background: var(--surface-2);
        color: var(--text-primary);
      }
      .detail-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 10px 18px;
      }
      .fact {
        display: flex;
        flex-direction: column;
        gap: 2px;
        min-width: 0;
      }
      .fact-label {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
      }
      .fact-value {
        font-size: 13px;
        color: var(--text-primary);
        overflow-wrap: anywhere;
      }
      .muted {
        color: var(--text-muted);
      }
      .detail-loading {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 12px;
        color: var(--text-muted);
      }
      .recovery-detail {
        display: flex;
        flex-wrap: wrap;
        gap: 6px 14px;
        padding: 10px;
        border: 1px solid var(--info);
        border-radius: var(--radius-control);
        color: var(--text-secondary);
        font-size: 12px;
      }
      .recovery-detail.attention {
        border-color: var(--warning);
      }
      .usage-block {
        display: flex;
        flex-direction: column;
        gap: 8px;
        padding-top: 10px;
        border-top: 1px solid var(--border-color);
      }
      .usage-head {
        display: flex;
        align-items: baseline;
        gap: 8px;
      }
      .usage-title {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
      }
      .usage-note {
        font-size: 11px;
        color: var(--text-muted);
      }
      .scope-switch {
        display: inline-flex;
        gap: 2px;
        padding: 2px;
        border-radius: var(--radius-control);
        background: var(--surface-0);
      }
      .scope-switch button {
        border: none;
        background: none;
        font: inherit;
        font-size: 11px;
        padding: 2px 8px;
        border-radius: var(--radius-control);
        color: var(--text-muted);
        cursor: pointer;
      }
      .scope-switch button:hover {
        color: var(--text-primary);
      }
      .scope-switch button.active {
        background: var(--surface-2);
        color: var(--text-primary);
      }
      .usage-figures {
        display: flex;
        gap: 28px;
      }
      .figure {
        display: flex;
        flex-direction: column;
        gap: 2px;
      }
      .figure-value {
        font-size: 18px;
        font-weight: 600;
        color: var(--text-primary);
        font-variant-numeric: tabular-nums;
      }
      .figure-qualifier {
        font-size: 13px;
        color: var(--text-muted);
        cursor: help;
      }
      .figure-label {
        font-size: 11px;
        color: var(--text-muted);
      }
      .usage-reason {
        margin: 0;
        font-size: 12px;
        color: var(--text-muted);
        line-height: 1.5;
      }
      .usage-models {
        margin: 0;
        padding: 0;
        list-style: none;
        display: flex;
        flex-direction: column;
        gap: 3px;
      }
      .usage-models li {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        font-size: 12px;
        color: var(--text-secondary);
      }
      .model-name {
        font-family: ui-monospace, monospace;
        overflow-wrap: anywhere;
      }
      .model-tokens {
        font-variant-numeric: tabular-nums;
        white-space: nowrap;
      }
      .detail-subjobs {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .subjobs-head {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 10px;
      }
      .subjobs-label {
        font-size: 11px;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--text-muted);
      }
      .subjobs-blocked {
        font-size: 12px;
        color: var(--warning);
      }
      .subjobs-reveal {
        background: none;
        border: none;
        padding: 0;
        cursor: pointer;
        font-size: 12px;
        color: var(--accent-color);
        text-decoration: underline;
      }
      .subjobs-reveal:hover {
        color: var(--accent-hover);
      }
      .subjob-row {
        cursor: pointer;
      }
      .subjob-row:hover {
        background: var(--hover);
      }
      .subjob-row td {
        padding: 5px 8px 5px 0;
        vertical-align: top;
      }
      /* The live row is the answer to "why is the parent waiting" — it earns
         the only accent in the table. */
      .subjob-live .sub-role {
        color: var(--accent-color);
        font-weight: 600;
      }
      .subagent-live .subagent-handle {
        color: var(--accent-color);
        font-weight: 600;
      }
      .sub-role {
        color: var(--text-primary);
        white-space: nowrap;
        width: 1%;
      }
      .sub-status {
        width: 1%;
        white-space: nowrap;
      }
      .subagent-type,
      .subagent-metrics,
      .subagent-link {
        width: 1%;
        white-space: nowrap;
      }
      .subagent-type,
      .subagent-metrics {
        color: var(--text-secondary);
      }
      .subagent-metrics {
        font-variant-numeric: tabular-nums;
      }
      .subagent-link a {
        color: var(--accent-color);
      }
      .sub-desc {
        color: var(--text-secondary);
        max-width: 0;
      }
      .sub-desc-text {
        display: block;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .sub-error {
        display: block;
        margin-top: 2px;
        color: var(--danger);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .sub-outcome {
        display: block;
        margin-top: 2px;
        color: var(--text-muted);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .sub-elapsed {
        width: 1%;
        white-space: nowrap;
        color: var(--text-muted);
        font-variant-numeric: tabular-nums;
      }
      .sub-id {
        width: 1%;
        white-space: nowrap;
      }
      .sub-id code {
        font-family: var(--font-mono);
        font-size: 10px;
        color: var(--text-muted);
      }
      @media (max-width: 720px) {
        /* Narrow: the description and the id are the first things to go —
           the role, status and elapsed are what answer the question. */
        .sub-desc,
        .sub-id,
        .subagent-metrics {
          display: none;
        }
      }
    `,
  ],
})
export class JobDetailPanelComponent {
  readonly job = input.required<JobSummary>();
  /** Null until the lazy load for this job has been kicked off. */
  readonly data = input<JobDetailState | null>(null);
  /**
   * Children the SERVER returned for this root — i.e. the ones matching the
   * current filter, not every child that exists. The copy says so; a bare count
   * here would repeat the exact overstatement the list avoids elsewhere.
   */
  readonly childCount = input(0);

  /**
   * Asked for when the reader first switches to the subtree scope. The parent
   * owns the fetch and the cache; the panel only says when it is wanted, so a
   * leaf job never pays for a request nobody looked at.
   */
  readonly subtreeRequested = output<void>();

  /** A roster row was clicked — the parent decides what "open" means. */
  readonly subjobSelected = output<string>();

  /**
   * The reader wants the hidden subjobs back as real list rows.
   *
   * The panel deliberately does not reach for the filter itself: the roster is
   * a read-only view of a job, while widening the origin filter changes the URL
   * and the whole page. That belongs to the list.
   */
  readonly revealRows = output<void>();

  /** Which figure is on screen. Resets with the panel, which is cheap and honest. */
  readonly scope = signal<UsageScope>('job');

  /** Whether the prompt is shown whole. Resets with the panel, like `scope`. */
  readonly descExpanded = signal(false);
  /** True once the clamped prompt is genuinely taller than its cap. */
  readonly descOverflowed = signal(false);
  private readonly descBody = viewChild<ElementRef<HTMLElement>>('descBody');

  constructor() {
    // "Show more" may only appear when text is actually hidden, and a
    // length heuristic lies in both directions — a hard-wrapped prompt
    // overflows where the same character count of flowing prose does not.
    // So measure the laid-out paragraph, as the tool card does.
    effect((onCleanup) => {
      const el = this.descBody()?.nativeElement;
      if (!el || typeof ResizeObserver === 'undefined') {
        this.descOverflowed.set(false);
        return;
      }
      const observer = new ResizeObserver(() => {
        // Expanding lifts the cap, so the element then measures as "fits" —
        // which would remove the very button needed to collapse it again.
        if (this.descExpanded()) return;
        this.descOverflowed.set(el.scrollHeight > el.clientHeight + 1);
      });
      observer.observe(el);
      onCleanup(() => observer.disconnect());
    });
  }

  protected readonly formatCount = formatCount;
  protected readonly formatUsd = formatUsd;
  protected readonly shortId = shortJobId;
  protected readonly isTerminal = isTerminalJobStatus;

  statusTone(status: string): BadgeTone {
    return jobStatusTone(status);
  }

  subagentTone(status: JobSubagentStatus): BadgeTone {
    return subagentStatusTone(status);
  }

  /**
   * Elapsed time for one roster row.
   *
   * A method rather than a computed because a live subjob's elapsed time is
   * measured against *now*: the value has to be recomputed on each change
   * detection pass, and the panel is refreshed every 30s by the list's poller
   * while the job is not terminal.
   */
  elapsedLabel(sub: JobSubjob): string | null {
    return formatDurationSeconds(subjobElapsedSeconds(sub, Date.now()));
  }

  subagentElapsedLabel(sub: JobSubagent): string | null {
    return formatDurationSeconds(subagentElapsedSeconds(sub, Date.now()));
  }

  /** The tree as the server knows it. Empty while loading or after a failure. */
  readonly subjobs = computed<JobSubjob[]>(() => this.data()?.subjobs ?? []);
  readonly subagents = computed<JobSubagent[]>(() => this.data()?.subagents ?? []);

  /** Subjobs still able to move on their own. */
  readonly liveCount = computed(() => liveSubjobCount(this.subjobs()));

  /** Set only for a `waiting` parent — see {@link subjobBlockedKey}. */
  readonly blockedKey = computed(() => subjobBlockedKey(this.job().status, this.subjobs()));

  /**
   * Subjobs the list is not showing as rows.
   *
   * The gap between the real tree and `childCount` IS the filter, made visible.
   * Under the default origin filter it equals the whole roster, which is
   * precisely the state that made a `waiting` parent look stalled.
   */
  readonly hiddenCount = computed(() => Math.max(0, this.subjobs().length - this.childCount()));

  setScope(scope: UsageScope): void {
    this.scope.set(scope);
    if (scope !== 'subtree') return;
    const state = this.data();
    // Ask once. Keyed off `subtreeAttempted`, not off the data: a failed call
    // leaves `usageSubtree` null, so a data-keyed guard would re-ask on every
    // click of a scope the server has already refused.
    if (state && !state.subtreeAttempted) {
      this.subtreeRequested.emit();
    }
  }

  readonly usage = computed(() => this.data()?.usage ?? null);
  /** Whichever scope the reader is looking at — everything below reads this. */
  readonly activeUsage = computed(() =>
    this.scope() === 'subtree' ? (this.data()?.usageSubtree ?? null) : this.usage(),
  );
  readonly cost = computed<CostDisplay>(() => costDisplay(this.activeUsage()));
  /**
   * How many jobs the subtree figure actually covers, straight from the server.
   *
   * Deliberately not `childCount`: that is the *filtered* count of children the
   * list happens to be showing, while the subtree sum walks the real tree in the
   * database. Conflating them would put a filter artifact into a spend figure.
   */
  readonly subtreeJobCount = computed(() => this.data()?.usageSubtree?.job_count ?? 0);
  readonly modelLabel = computed(() => jobModelLabel(this.data()?.detail ?? null));

  readonly durationLabel = computed(() =>
    formatDurationSeconds(this.data()?.progress?.elapsed_seconds),
  );

  /**
   * Backend from the row's own safe projection — no extra call, and nothing
   * here can leak a workspace lease identity (acceptance 5). Effective wins
   * over assigned wins over requested: the last one is only an intent.
   */
  readonly workspaceLabel = computed(() =>
    workspaceBackendLabel(this.job().workspace_contract),
  );

  /** Token totals per model, biggest first — the per-resource split, folded. */
  readonly perModel = computed(() => {
    const rows = this.activeUsage()?.rows ?? [];
    const byResource = new Map<string, number>();
    for (const row of rows) {
      if (row.category !== 'llm') continue;
      if (!row.unit.endsWith('-token')) continue;
      byResource.set(row.resource, (byResource.get(row.resource) ?? 0) + row.quantity);
    }
    return [...byResource.entries()]
      .map(([resource, tokens]) => ({resource, tokens}))
      .sort((a, b) => b.tokens - a.tokens);
  });
}
