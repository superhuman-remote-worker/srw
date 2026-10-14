import {ChangeDetectionStrategy, Component, inject, OnInit, signal} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {SettingsService} from '../../../core/services/settings.service';
import type {
  MainCloudCell,
  MainCloudHelmState,
  MainCloudMatrixRow,
  MainCloudPage,
} from '../../../core/models/main-cloud.model';
import {AppBadgeComponent, type BadgeTone} from '../../../ui/badge';
import {AppButtonComponent} from '../../../ui/button';
import {AppIconComponent} from '../../../ui/icon';
import {AppSpinnerComponent} from '../../../ui/spinner';

const STATUS_TONE: Record<MainCloudCell['status'], BadgeTone> = {
  offered: 'success',
  planned: 'info',
  unsupported: 'neutral',
};
const HELM_TONE: Record<MainCloudHelmState, BadgeTone> = {
  matches: 'success',
  differs: 'warning',
  invalid: 'danger',
};
/** Largest unit first: the relative label of a past moment uses the first that fits. */
const RELATIVE_UNITS: [Intl.RelativeTimeFormatUnit, number][] = [
  ['day', 86_400],
  ['hour', 3_600],
  ['minute', 60],
  ['second', 1],
];

/**
 * Admin Settings → Main cloud (main_cloud_as_connectors.md, slice 2).
 *
 * The main cloud is configured by Helm only, so this page has no form: it
 * shows the active provider, its public URL, the installation id, health,
 * whether Helm's values match the active installation, and the provider
 * support matrix each adapter declares. A combination a provider cannot do
 * is shown as unsupported with the reason, never offered with an asterisk.
 *
 * For a provider with background jobs (Nextcloud) it also shows their health
 * (mode, last run, stale) and the trash and version retention the chart pins
 * (bundled_nextcloud_performance.md, D13), read-only like the rest.
 */
@Component({
  selector: 'app-main-cloud-settings',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoPipe, AppBadgeComponent, AppButtonComponent, AppIconComponent, AppSpinnerComponent],
  template: `
    @if (loadFailed()) {
      <div class="error-banner" role="alert">
        <span>{{ 'settings.cloud.loadFailed' | transloco }}</span>
        <app-button variant="secondary" size="sm" (clicked)="load()">
          {{ 'settings.cloud.retry' | transloco }}
        </app-button>
      </div>
    } @else if (page(); as p) {
      <div class="status-card" data-section="status">
        <span class="status-dot" [class.ok]="p.health.ok"></span>
        <span class="status-text">
          {{ 'settings.cloud.active' | transloco }}
          <strong>{{ p.provider.title }}</strong>
          &mdash;
          @if (p.health.ok) {
            {{ 'settings.cloud.healthy' | transloco: {ms: p.health.latency_ms} }}
          } @else if (!p.provider.initialized) {
            {{ 'settings.cloud.notInitialized' | transloco }}
          } @else {
            {{ 'settings.cloud.unreachable' | transloco }}
          }
        </span>
        <app-button
          variant="ghost"
          size="sm"
          [ariaLabel]="'settings.cloud.refresh' | transloco"
          (clicked)="load()"
        >
          <app-icon size="sm">refresh</app-icon>
        </app-button>
      </div>

      <dl class="facts" data-section="facts">
        <dt>{{ 'settings.cloud.provider' | transloco }}</dt>
        <dd>{{ p.provider.title }} <code>{{ p.provider.backend_id }}</code></dd>

        <dt>{{ 'settings.cloud.publicUrl' | transloco }}</dt>
        <dd>
          @if (p.provider.public_url) {
            <a [href]="p.provider.public_url" target="_blank" rel="noopener">{{ p.provider.public_url }}</a>
          } @else {
            <span class="muted">{{ 'settings.cloud.unknown' | transloco }}</span>
          }
        </dd>

        <dt>{{ 'settings.cloud.installation' | transloco }}</dt>
        <dd data-fact="installation">
          @if (p.provider.backend_instance_id) {
            <code>{{ p.provider.backend_instance_id }}</code>
            <span class="sub">
              {{ 'settings.cloud.activation' | transloco: {revision: p.provider.activation_revision} }}
              @if (p.provider.activated_at) {
                · {{ activatedAt(p) }}
              }
            </span>
          } @else {
            <span class="muted">{{ 'settings.cloud.noInstallation' | transloco }}</span>
          }
        </dd>

        <dt>{{ 'settings.cloud.configuration' | transloco }}</dt>
        <dd data-fact="configuration">
          {{ 'settings.cloud.helmOnly' | transloco }}
          <app-badge [tone]="helmTone(p)" size="xs">
            {{ 'settings.cloud.helm.' + p.configuration.helm.state | transloco }}
          </app-badge>
        </dd>
      </dl>

      @if (p.configuration.helm.state !== 'matches') {
        <p class="helm-note" [attr.data-helm]="p.configuration.helm.state">
          {{ 'settings.cloud.helmNote.' + p.configuration.helm.state | transloco: {instance: p.provider.backend_instance_id} }}
        </p>
      }

      @if (p.background_jobs; as jobs) {
        <h3 class="section-title">{{ 'settings.cloud.jobs.title' | transloco }}</h3>
        <dl class="facts" data-section="background-jobs" [class.stale]="jobs.stale">
          <dt>{{ 'settings.cloud.jobs.mode' | transloco }}</dt>
          <dd data-fact="mode">
            @if (jobs.mode) {
              <code>{{ jobs.mode }}</code>
              @if (jobs.mode === 'ajax') {
                <span class="sub">{{ 'settings.cloud.jobs.ajaxNote' | transloco }}</span>
              }
            } @else {
              <span class="muted">{{ 'settings.cloud.unknown' | transloco }}</span>
            }
          </dd>

          <dt>{{ 'settings.cloud.jobs.lastRun' | transloco }}</dt>
          <dd data-fact="last-run">
            @if (jobs.last_run_at) {
              <time [attr.datetime]="jobs.last_run_at" [attr.title]="absoluteTime(jobs.last_run_at)">
                {{ relativeTime(jobs.last_run_at) }}
              </time>
            } @else if (jobs.mode) {
              <span class="muted">{{ 'settings.cloud.jobs.never' | transloco }}</span>
            } @else {
              <span class="muted">{{ 'settings.cloud.unknown' | transloco }}</span>
            }
            @if (jobs.stale) {
              <app-badge tone="warning" size="xs">{{ 'settings.cloud.jobs.stale' | transloco }}</app-badge>
            }
          </dd>

          @if (jobs.cron_container !== null) {
            <dt>{{ 'settings.cloud.jobs.cronContainer' | transloco }}</dt>
            <dd data-fact="cron-container">
              {{ (jobs.cron_container ? 'settings.cloud.jobs.cronOn' : 'settings.cloud.jobs.cronOff') | transloco }}
            </dd>
          }

          @if (p.retention; as retention) {
            @if (retention.source === 'chart') {
              <dt>{{ 'settings.cloud.jobs.trashbin' | transloco }}</dt>
              <dd data-fact="trashbin"><code>{{ retention.trashbin ?? '—' }}</code></dd>

              <dt>{{ 'settings.cloud.jobs.versions' | transloco }}</dt>
              <dd data-fact="versions"><code>{{ retention.versions ?? '—' }}</code></dd>
            } @else {
              <dt>{{ 'settings.cloud.jobs.retention' | transloco }}</dt>
              <dd data-fact="retention" class="muted">{{ 'settings.cloud.jobs.retentionExternal' | transloco }}</dd>
            }
          }
        </dl>

        @if (p.retention?.source === 'chart') {
          <p class="retention-note" data-note="retention">
            {{ 'settings.cloud.jobs.retentionChart' | transloco }}
            @if (retentionDisabled(p)) {
              {{ 'settings.cloud.jobs.retentionDisabled' | transloco }}
            }
          </p>
        }

        @if (jobs.stale) {
          <p class="helm-note" data-jobs="stale">{{ 'settings.cloud.jobs.staleNote' | transloco }}</p>
        }
      }

      <h3 class="matrix-title">{{ 'settings.cloud.matrixTitle' | transloco }}</h3>
      <p class="matrix-desc">{{ 'settings.cloud.matrixDesc' | transloco }}</p>
      <div class="matrix-scroll">
        <table class="app-table matrix">
          <thead>
            <tr>
              <th scope="col">{{ 'settings.cloud.connector' | transloco }}</th>
              @for (provider of p.matrix.providers; track provider.backend_id) {
                <th scope="col" [class.active]="provider.active" [attr.data-provider]="provider.backend_id">
                  {{ provider.title }}
                  @if (provider.active) {
                    <app-badge tone="accent" size="xs">{{ 'settings.cloud.activeBadge' | transloco }}</app-badge>
                  }
                </th>
              }
            </tr>
          </thead>
          <tbody>
            @for (row of p.matrix.rows; track rowKey(row)) {
              <tr [attr.data-row]="rowKey(row)">
                <th scope="row" class="combination">
                  <span class="type">{{ label('types', row.connector_type) }}</span>
                  @if (row.folder_kind) {
                    <span class="sep">·</span>
                    <span class="kind">{{ label('kinds', row.folder_kind) }}</span>
                  }
                  <span class="sep">·</span>
                  <code class="access">{{ row.access }}</code>
                </th>
                @for (provider of p.matrix.providers; track provider.backend_id) {
                  @if (row.cells[provider.backend_id]; as cell) {
                    <td [attr.data-cell]="provider.backend_id" [attr.data-status]="cell.status">
                      <app-badge [tone]="statusTone(cell)" size="xs">
                        @if (cell.status === 'planned') {
                          {{ 'settings.cloud.status.planned' | transloco: {slice: cell.slice} }}
                        } @else {
                          {{ 'settings.cloud.status.' + cell.status | transloco }}
                        }
                      </app-badge>
                      @if (cell.workspace_backends.length > 0) {
                        <span class="tiers">{{ cell.workspace_backends.join(', ') }}</span>
                      }
                      <span class="note">{{ cell.note }}</span>
                    </td>
                  }
                }
              </tr>
            }
          </tbody>
        </table>
      </div>
    } @else {
      <div class="loading"><app-spinner size="sm" /> {{ 'settings.cloud.loading' | transloco }}</div>
    }
  `,
  styles: [
    `
      :host {
        display: flex;
        flex-direction: column;
        gap: 12px;
        min-width: 0;
      }

      .error-banner {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 10px 12px;
        border-radius: var(--radius-surface);
        background: color-mix(in srgb, var(--danger) 10%, transparent);
        color: var(--danger);
      }

      .loading {
        display: flex;
        align-items: center;
        gap: 8px;
        color: var(--text-muted);
      }

      .status-card {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 12px 16px;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
      }

      .status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--text-muted);
        flex-shrink: 0;
      }

      .status-dot.ok {
        background: var(--color-success, #10b981);
      }

      .status-text {
        flex: 1;
        font-size: 13px;
        color: var(--text-secondary);
      }

      .facts {
        display: grid;
        grid-template-columns: max-content minmax(0, 1fr);
        gap: 6px 16px;
        margin: 0;
        font-size: 0.85rem;
      }

      .facts dt {
        font-weight: 600;
        color: var(--text-secondary);
      }

      .facts dd {
        margin: 0;
        min-width: 0;
        overflow-wrap: anywhere;
      }

      .facts a {
        color: var(--accent-color);
      }

      code {
        font-family: var(--font-mono);
        font-size: 0.78rem;
        overflow-wrap: anywhere;
      }

      .sub {
        display: block;
        color: var(--text-muted);
        font-size: 0.8rem;
      }

      .muted {
        color: var(--text-muted);
      }

      .helm-note {
        margin: 0;
        padding: 8px 12px;
        border-left: 2px solid var(--warning);
        font-size: 0.85rem;
        color: var(--text-secondary);
      }

      .matrix-title,
      .section-title {
        margin: 8px 0 0;
        font-size: 0.95rem;
      }

      .facts.stale [data-fact='last-run'] time {
        color: var(--warning);
      }

      .retention-note {
        margin: 0;
        font-size: 0.8rem;
        color: var(--text-muted);
      }

      .matrix-desc {
        margin: 0;
        font-size: 0.85rem;
        color: var(--text-secondary);
      }

      .matrix-scroll {
        max-width: 100%;
        overflow-x: auto;
        border: 1px solid var(--border-hairline);
        border-radius: var(--radius-surface);
      }

      .matrix {
        min-width: 560px;
      }

      .matrix tbody th {
        padding: 10px 12px;
        text-align: left;
        border-bottom: 1px solid var(--border-hairline);
      }

      .matrix tbody th,
      .matrix tbody td {
        vertical-align: top;
      }

      .matrix tbody tr:last-child th,
      .matrix tbody tr:last-child td {
        border-bottom: none;
      }

      .matrix thead th.active {
        color: var(--text-primary);
      }

      .combination {
        font-weight: 500;
        white-space: nowrap;
      }

      .sep {
        margin: 0 4px;
        color: var(--text-muted);
      }

      .tiers {
        display: block;
        margin-top: 4px;
        font-family: var(--font-mono);
        font-size: 0.74rem;
        color: var(--text-secondary);
      }

      .note {
        display: block;
        margin-top: 2px;
        color: var(--text-muted);
        font-size: 0.78rem;
        line-height: 1.35;
      }

      @media (max-width: 640px) {
        .facts {
          grid-template-columns: minmax(0, 1fr);
        }

        .facts dd {
          margin-bottom: 6px;
        }
      }
    `,
  ],
})
export class MainCloudSettingsComponent implements OnInit {
  private readonly settingsService = inject(SettingsService);
  private readonly transloco = inject(TranslocoService);

  readonly page = signal<MainCloudPage | null>(null);
  readonly loadFailed = signal(false);
  /**
   * When the page arrived: relative times are as of then (Refresh renews
   * them), so a label never changes between dev mode's two
   * change-detection passes (NG0100).
   */
  private readonly loadedAt = signal(Date.now());

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loadFailed.set(false);
    this.settingsService.getMainCloud().subscribe({
      next: (page) => {
        this.loadedAt.set(Date.now());
        this.page.set(page);
      },
      error: () => this.loadFailed.set(true),
    });
  }

  protected rowKey(row: MainCloudMatrixRow): string {
    return [row.connector_type, row.folder_kind ?? '-', row.access].join('/');
  }

  protected statusTone(cell: MainCloudCell): BadgeTone {
    return STATUS_TONE[cell.status] ?? 'neutral';
  }

  protected helmTone(page: MainCloudPage): BadgeTone {
    return HELM_TONE[page.configuration.helm.state] ?? 'warning';
  }

  /** A known id's label, else the id itself (a newer server's vocabulary). */
  protected label(group: 'types' | 'kinds', id: string): string {
    const key = `settings.cloud.${group}.${id}`;
    const text = this.transloco.translate(key);
    return text && text !== key ? text : id;
  }

  protected activatedAt(page: MainCloudPage): string {
    const value = page.provider.activated_at;
    return value ? this.absoluteTime(value) : '';
  }

  protected absoluteTime(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
  }

  /** "3 minutes ago" in the active language, as of when the page loaded. */
  protected relativeTime(value: string): string {
    const time = new Date(value).getTime();
    if (Number.isNaN(time)) return value;
    // A moment slightly ahead of this browser's clock is "now", not "in 5 seconds".
    const seconds = Math.min(0, Math.trunc((time - this.loadedAt()) / 1000));
    const [unit, size] =
      RELATIVE_UNITS.find(([, length]) => -seconds >= length) ?? RELATIVE_UNITS[3];
    const format = new Intl.RelativeTimeFormat(this.transloco.getActiveLang(), {numeric: 'auto'});
    return format.format(Math.trunc(seconds / size), unit);
  }

  /** Whether the chart pins either retention to `disabled` (never expire). */
  protected retentionDisabled(page: MainCloudPage): boolean {
    const retention = page.retention;
    return (
      retention?.source === 'chart' &&
      (retention.trashbin === 'disabled' || retention.versions === 'disabled')
    );
  }
}
