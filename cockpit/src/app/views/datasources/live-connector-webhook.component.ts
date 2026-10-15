import {ChangeDetectionStrategy, Component, computed, input, signal} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {environment} from '../../core/environment';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppInputComponent} from '../../ui/input';
import {AppButtonComponent} from '../../ui/button';
import {AppIconComponent} from '../../ui/icon';

/** The API origin may differ from the origin serving Cockpit. */
export function liveConnectorWebhookUrl(connectorId: string): string {
  const base = new URL(environment.apiUrl, typeof window === 'undefined' ? 'http://localhost' : window.location.origin);
  return `${base.origin}/api/live/webhooks/${encodeURIComponent(connectorId)}`;
}

@Component({
  selector: 'app-live-connector-webhook',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoPipe, AppFormFieldComponent, AppInputComponent, AppButtonComponent, AppIconComponent],
  template: `
    <app-form-field [label]="'datasources.liveWebhook.label' | transloco"
      [hint]="'datasources.liveWebhook.help' | transloco">
      <app-input [value]="url()" [readonly]="true"
        [ariaLabel]="'datasources.liveWebhook.label' | transloco" />
      <app-button formFieldAction variant="ghost" size="sm" (clicked)="copy()">
        <app-icon size="sm">{{ copied() ? 'check' : 'content_copy' }}</app-icon>
        {{ (copied() ? 'datasources.liveWebhook.copied' : 'datasources.liveWebhook.copy') | transloco }}
      </app-button>
      @if (copyFailed()) {
        <span role="status">{{ 'datasources.liveWebhook.copyFailed' | transloco }}</span>
      }
    </app-form-field>
  `,
})
export class LiveConnectorWebhookComponent {
  readonly connectorId = input.required<string>();
  readonly url = computed(() => liveConnectorWebhookUrl(this.connectorId()));
  readonly copied = signal(false);
  readonly copyFailed = signal(false);

  async copy(): Promise<void> {
    this.copied.set(false);
    this.copyFailed.set(false);
    try {
      await navigator.clipboard.writeText(this.url());
      this.copied.set(true);
    } catch {
      this.copyFailed.set(true);
    }
  }
}
