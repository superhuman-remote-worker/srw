import {ChangeDetectionStrategy, Component, computed, inject, OnInit} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {AppButtonComponent} from '../../ui/button';
import {AppBadgeComponent, type BadgeTone} from '../../ui/badge';
import {CloudRootService} from '../../core/services/cloud-root.service';
import {CloudRootStatus} from '../../core/models/api.model';

const FAILURES = new Set([
  'wrong_account',
  'admin_account',
  'account_disabled',
  'verify_failed',
  'expired',
  'provider_error',
  'account_missing',
  'error',
]);
const UNSUPPORTED = new Set(['provider_unsupported', 'grant_mode_off', 'no_personal_project']);

/** The line under "My cloud" for a state: who it is connected as, why a
 *  connection failed or was revoked, or why there is none to make. */
export function cloudRootDetail(status: CloudRootStatus): {key: string; params?: Record<string, string>} | null {
  switch (status.state) {
    case 'connected':
      return status.account_id
        ? {key: 'cloudRoot.detail.connectedAs', params: {account: status.account_id}}
        : {key: 'cloudRoot.detail.connected'};
    case 'connecting':
      return {key: 'cloudRoot.detail.connecting'};
    case 'not_connected':
      return {key: 'cloudRoot.detail.notConnected'};
    case 'failed': {
      const reason = String(status.failure?.reason ?? 'error');
      return {key: `cloudRoot.failure.${FAILURES.has(reason) ? reason : 'error'}`};
    }
    case 'revoked':
      return {
        key: status.revoked_reason === 'rejected_by_cloud'
          ? 'cloudRoot.revoked.rejected_by_cloud'
          : 'cloudRoot.revoked.user',
      };
    case 'unsupported': {
      const reason = String(status.reason ?? '');
      return {key: `cloudRoot.unsupported.${UNSUPPORTED.has(reason) ? reason : 'provider_unsupported'}`};
    }
    default:
      return null;
  }
}

/**
 * "My cloud" in Settings → Connectors: the user's own cloud root, a
 * connector SRW keeps for them (main cloud slice 4). It shows where the
 * connection stands and the one action that moves it: Connect (the cloud's
 * own sign-in, in a new tab), Cancel while it waits, Disconnect, or
 * Reconnect once the cloud stopped accepting it. Hidden where there is no
 * main cloud at all.
 */
@Component({
  selector: 'app-cloud-root-card',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoPipe, AppIconComponent, AppButtonComponent, AppBadgeComponent],
  template: `
    @if (visible() && status(); as s) {
      <section class="cloud-root" data-cloud-root [attr.data-state]="s.state" aria-labelledby="cloud-root-title">
        <app-icon size="md" class="cloud-root__icon" aria-hidden="true">cloud</app-icon>
        <div class="cloud-root__body">
          <div class="cloud-root__head">
            <strong id="cloud-root-title">{{ 'datasources.platform.kind.userCloudRoot' | transloco }}</strong>
            <app-badge [tone]="stateTone()" size="xs" data-cloud-root-state>
              {{ 'cloudRoot.state.' + s.state | transloco }}
            </app-badge>
            <span class="cloud-root__managed">{{ 'datasources.platform.managedForYou' | transloco }}</span>
          </div>
          <p class="cloud-root__desc">{{ 'cloudRoot.description' | transloco }}</p>
          @if (detail(); as line) {
            <p class="cloud-root__detail" [class.cloud-root__detail--warn]="s.state === 'failed' || s.state === 'revoked'" data-cloud-root-detail>
              {{ line.key | transloco: line.params ?? {} }}
            </p>
          }
          @if (s.state === 'connecting' && s.connecting) {
            <a class="cloud-root__link" [href]="s.connecting.login_url" target="_blank" rel="noopener noreferrer" data-cloud-root-login>
              {{ 'cloudRoot.openLogin' | transloco }}
            </a>
          }
          @if (cloudRoot.revokeIncomplete()) {
            <p class="cloud-root__detail cloud-root__detail--warn" data-cloud-root-revoke-incomplete>
              {{ 'cloudRoot.revokeIncomplete' | transloco: {client: s.client_name || 'SRW'} }}
            </p>
          }
          @if (cloudRoot.actionError(); as errorKey) {
            <p class="cloud-root__detail cloud-root__detail--error" role="alert" data-cloud-root-error>{{ errorKey | transloco }}</p>
          }
        </div>
        <div class="cloud-root__actions">
          @switch (s.state) {
            @case ('connected') {
              <app-button variant="secondary" size="sm" [loading]="cloudRoot.busy()" (clicked)="disconnect()" data-cloud-root-action="disconnect">
                {{ 'cloudRoot.actions.disconnect' | transloco }}
              </app-button>
            }
            @case ('connecting') {
              <app-button variant="secondary" size="sm" [loading]="cloudRoot.busy()" (clicked)="cloudRoot.cancel()" data-cloud-root-action="cancel">
                {{ 'cloudRoot.actions.cancel' | transloco }}
              </app-button>
            }
            @case ('revoked') {
              <app-button variant="primary" size="sm" [loading]="cloudRoot.busy()" (clicked)="cloudRoot.connect()" data-cloud-root-action="reconnect">
                {{ 'cloudRoot.actions.reconnect' | transloco }}
              </app-button>
            }
            @case ('not_connected') {
              <app-button variant="primary" size="sm" [loading]="cloudRoot.busy()" (clicked)="cloudRoot.connect()" data-cloud-root-action="connect">
                {{ 'cloudRoot.actions.connect' | transloco }}
              </app-button>
            }
            @case ('failed') {
              <app-button variant="primary" size="sm" [loading]="cloudRoot.busy()" (clicked)="cloudRoot.connect()" data-cloud-root-action="connect">
                {{ 'cloudRoot.actions.connect' | transloco }}
              </app-button>
            }
          }
        </div>
      </section>
    }
  `,
  styles: [`
    :host { display: block; }
    .cloud-root {
      display: flex;
      align-items: flex-start;
      gap: 12px;
      margin: 0 16px 12px;
      padding: 12px 14px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      background: var(--surface-0);
    }
    .cloud-root__icon { color: var(--info); flex-shrink: 0; margin-top: 2px; }
    .cloud-root__body { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 4px; }
    .cloud-root__head { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
    .cloud-root__managed { font-size: 12px; color: var(--text-muted); }
    .cloud-root__desc,
    .cloud-root__detail { margin: 0; font-size: 13px; color: var(--text-secondary); }
    .cloud-root__detail--warn { color: var(--warning); }
    .cloud-root__detail--error { color: var(--danger); }
    .cloud-root__link { font-size: 13px; color: var(--accent-color); }
    .cloud-root__actions { flex-shrink: 0; display: flex; gap: 8px; }
    @media (max-width: 640px) {
      .cloud-root { flex-wrap: wrap; margin: 0 12px 12px; }
      .cloud-root__actions { width: 100%; }
    }
  `],
})
export class CloudRootCardComponent implements OnInit {
  readonly cloudRoot = inject(CloudRootService);
  private readonly transloco = inject(TranslocoService);

  readonly status = this.cloudRoot.status;

  /** Nothing to show where the deployment has no main cloud. */
  readonly visible = computed(() => {
    const s = this.status();
    return !!s && !(s.state === 'unsupported' && s.reason === 'no_main_cloud');
  });

  readonly detail = computed(() => {
    const s = this.status();
    return s ? cloudRootDetail(s) : null;
  });

  readonly stateTone = computed<BadgeTone>(() => {
    switch (this.status()?.state) {
      case 'connected':
        return 'success';
      case 'connecting':
        return 'info';
      case 'failed':
      case 'revoked':
        return 'warning';
      default:
        return 'neutral';
    }
  });

  ngOnInit(): void {
    this.cloudRoot.load();
  }

  disconnect(): void {
    if (!confirm(this.transloco.translate('cloudRoot.disconnectConfirm'))) return;
    this.cloudRoot.disconnect();
  }
}
