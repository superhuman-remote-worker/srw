import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  computed,
  inject,
  OnInit,
  signal,
} from '@angular/core';
import {firstValueFrom} from 'rxjs';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {SidebarToggleComponent} from '../../../shell/sidebar-toggle/sidebar-toggle.component';
import {ApiService} from '../../../core/services/api.service';
import {AppToastService} from '../../../ui/toast';
import {AdminCapacity, AdminCapacityParkedRow} from '../../../core/models/api.model';
import {queueParkReasonKey} from '../../../core/models/queue-park-reason';
import {AdminVMCapacityComponent} from './admin-vm-capacity.component';

/** How often the page re-reads GET /api/admin/capacity while open. */
export const CAPACITY_REFRESH_MS = 10_000;

/**
 * Admin → Capacity (stateless_turn_resilience.md, step 2). The operator view
 * of what the KEDA scaler sees — executors, runnable queue depth, `desired` —
 * plus the parked worklist with a one-click Unpark. Queue depth is operator
 * information: end users only ever see their own unit's state.
 */
@Component({
  selector: 'app-admin-capacity',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [SidebarToggleComponent, TranslocoPipe, AdminVMCapacityComponent],
  template: `
    <div class="admin-page">
      <div class="admin-container">
        <div class="page-header">
          <app-sidebar-toggle />
          <h1 class="page-title">{{ 'admin.capacity.title' | transloco }}</h1>
          <div class="page-controls">
            @if (capacity(); as c) {
              <span class="observed">{{ 'admin.capacity.observedAt' | transloco: {time: fmtTime(c.observed_at)} }}</span>
            }
          </div>
        </div>
        <p class="page-desc">{{ 'admin.capacity.desc' | transloco }}</p>

        @if (loadFailed()) {
          <p class="load-failed" role="alert">{{ 'admin.capacity.loadFailed' | transloco }}</p>
        }

        @if (capacity(); as c) {
          <section class="kpi-row" data-testid="capacity-kpis">
            <div class="kpi-card">
              <span class="kpi-label">{{ 'admin.capacity.executors' | transloco }}</span>
              <span class="kpi-value">{{ c.executors.total ?? '–' }}</span>
              <span class="kpi-sub">{{ c.executors.ready ?? '–' }} {{ 'admin.capacity.ready' | transloco }} · {{ c.executors.busy }} {{ 'admin.capacity.busy' | transloco }}</span>
            </div>
            <div class="kpi-card">
              <span class="kpi-label">{{ 'admin.capacity.queued' | transloco }}</span>
              <span class="kpi-value">{{ c.queued.total }}</span>
              <span class="kpi-sub">{{ c.queued.session_turn }} {{ 'admin.capacity.sessions' | transloco }} · {{ c.queued.worker_batch }} {{ 'admin.capacity.batches' | transloco }}</span>
            </div>
            <div class="kpi-card">
              <span class="kpi-label">{{ 'admin.capacity.oldestQueued' | transloco }}</span>
              <span class="kpi-value">{{ fmtAge(c.oldest_queued_age_s) }}</span>
            </div>
            <div class="kpi-card">
              <span class="kpi-label">{{ 'admin.capacity.desired' | transloco }}</span>
              <span class="kpi-value">{{ c.desired }}</span>
              <span class="kpi-sub">{{ 'admin.capacity.floor' | transloco }} {{ c.params.min_replicas }} · {{ 'admin.capacity.reserve' | transloco }} {{ c.params.reserve }}</span>
            </div>
          </section>

          @if (c.vm; as vm) {
            <app-admin-vm-capacity [vm]="vm" />
          }

          <section class="admin-section">
            <div class="section-head">
              <h2 class="section-title">{{ 'admin.capacity.parked' | transloco }} ({{ parked().length }})</h2>
            </div>
            @if (parked().length === 0) {
              <p class="section-note">{{ 'admin.capacity.none' | transloco }}</p>
            } @else {
              <div class="table-wrap">
                <table class="parked-table app-table" data-testid="parked-table">
                  <thead>
                    <tr>
                      <th>{{ 'admin.capacity.colTitle' | transloco }}</th>
                      <th>{{ 'admin.capacity.colOwner' | transloco }}</th>
                      <th>{{ 'admin.capacity.colKind' | transloco }}</th>
                      <th>{{ 'admin.capacity.colReason' | transloco }}</th>
                      <th>{{ 'admin.capacity.colParkedAt' | transloco }}</th>
                      <th>{{ 'admin.capacity.colAttempts' | transloco }}</th>
                      <th>{{ 'admin.capacity.colPending' | transloco }}</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    @for (row of parked(); track row.unit_id) {
                      <tr>
                        <td class="cell-title">
                          @if (row.thread_id) {
                            <a [href]="'/sessions/' + row.thread_id">{{ row.title || row.unit_id.slice(0, 8) }}</a>
                          } @else {
                            {{ row.title || row.unit_id.slice(0, 8) }}
                          }
                        </td>
                        <td>{{ row.owner || '–' }}</td>
                        <td>{{ row.unit_kind }}</td>
                        <td class="cell-reason">
                          <span class="reason-code">{{ row.park_reason || '–' }}</span>
                          <span class="reason-text">{{ parkReasonKey(row.park_reason) | transloco }}</span>
                        </td>
                        <td>{{ fmtTime(row.parked_at) }}</td>
                        <td>{{ row.attempts }}</td>
                        <td>{{ (row.pending_input ? 'admin.capacity.yes' : 'admin.capacity.no') | transloco }}</td>
                        <td>
                          <button type="button" class="unpark-btn" data-testid="unpark"
                                  [disabled]="busyUnit() === row.unit_id"
                                  (click)="unpark(row)">{{ 'admin.capacity.unpark' | transloco }}</button>
                        </td>
                      </tr>
                    }
                  </tbody>
                </table>
              </div>
            }
          </section>
        }
      </div>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow: auto;
      }
      .admin-page {
        padding: 32px;
        max-width: var(--content-max-width);
        margin: 0 auto;
        color: var(--text-primary);
      }
      .page-header {
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
        margin-bottom: 8px;
      }
      .page-controls {
        margin-left: auto;
        font-size: 12px;
        color: var(--text-muted);
      }
      .page-desc {
        color: var(--text-secondary);
        margin: 0 0 20px;
      }
      .load-failed {
        color: var(--danger);
        margin: 0 0 16px;
      }
      .kpi-row {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
        margin-bottom: 24px;
      }
      .kpi-card {
        display: flex;
        flex-direction: column;
        gap: 4px;
        padding: 14px 16px;
        border: 1px solid var(--border-hairline);
        border-radius: var(--radius-surface);
        background: var(--panel-bg);
      }
      .kpi-label {
        font-size: 12px;
        font-weight: 600;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .kpi-value {
        font-size: 26px;
        font-weight: 600;
        font-variant-numeric: tabular-nums;
      }
      .kpi-sub {
        font-size: 12px;
        color: var(--text-secondary);
      }
      .section-head {
        display: flex;
        align-items: baseline;
        gap: 12px;
        margin-bottom: 8px;
      }
      .section-title {
        font-size: 16px;
        margin: 0;
      }
      .section-note {
        color: var(--text-secondary);
        margin: 0;
      }
      .table-wrap {
        overflow-x: auto;
      }
      .parked-table th,
      .parked-table td {
        vertical-align: top;
        white-space: nowrap;
      }
      .cell-title a {
        color: var(--text-primary);
      }
      /* Must outrank the .parked-table td rule above, or the reason never wraps and
         the table runs past its wrapper (the Unpark button was clipped at 1440px). */
      .parked-table td.cell-reason {
        white-space: normal;
        min-width: 220px;
      }
      .reason-code {
        display: block;
        font-family: var(--font-mono);
        font-size: 12px;
        color: var(--text-muted);
      }
      .reason-text {
        color: var(--text-secondary);
      }
      .unpark-btn {
        padding: 4px 10px;
        border-radius: var(--radius-control);
        border: 1px solid var(--border-color);
        background: var(--surface-0);
        color: var(--text-primary);
        cursor: pointer;
      }
      .unpark-btn:disabled {
        opacity: 0.5;
        cursor: default;
      }
      @media (max-width: 640px) {
        .admin-page {
          padding: 16px;
        }
      }
    `,
  ],
})
export class AdminCapacityComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(AppToastService);
  private readonly transloco = inject(TranslocoService);
  private readonly destroyRef = inject(DestroyRef);

  readonly capacity = signal<AdminCapacity | null>(null);
  readonly loadFailed = signal(false);
  readonly busyUnit = signal<string | null>(null);
  readonly parked = computed<AdminCapacityParkedRow[]>(() => this.capacity()?.parked ?? []);

  private timer: ReturnType<typeof setInterval> | null = null;

  ngOnInit(): void {
    void this.load();
    this.timer = setInterval(() => void this.load(), CAPACITY_REFRESH_MS);
    this.destroyRef.onDestroy(() => {
      if (this.timer !== null) clearInterval(this.timer);
      this.timer = null;
    });
  }

  async load(): Promise<void> {
    const data = await firstValueFrom(this.api.getAdminCapacity());
    if (data === null) {
      this.loadFailed.set(true);
      return;
    }
    this.loadFailed.set(false);
    this.capacity.set(data);
  }

  async unpark(row: AdminCapacityParkedRow): Promise<void> {
    this.busyUnit.set(row.unit_id);
    try {
      const result = await firstValueFrom(this.api.unparkRunQueueUnit(row.unit_id));
      if (result === null) {
        this.toast.danger(this.transloco.translate('admin.capacity.unparkFailed'));
        return;
      }
      this.toast.success(this.transloco.translate('admin.capacity.unparked'));
      await this.load();
    } finally {
      this.busyUnit.set(null);
    }
  }

  parkReasonKey(reason: string | null): string {
    return queueParkReasonKey(reason);
  }

  fmtTime(iso: string | null | undefined): string {
    if (!iso) return '–';
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleTimeString();
  }

  fmtAge(seconds: number | null | undefined): string {
    const s = Math.max(0, Math.floor(seconds ?? 0));
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    return m < 60 ? `${m}m ${s % 60}s` : `${Math.floor(m / 60)}h ${m % 60}m`;
  }
}
