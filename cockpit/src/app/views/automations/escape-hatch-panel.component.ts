import {ChangeDetectionStrategy, Component} from '@angular/core';
import {RouterLink} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {environment} from '../../core/environment';

export const API_GUIDE_URL = 'https://docs.superhuman-remote-worker.com/api/';

/**
 * The "Need more than schedules?" panel at the bottom of the Automations
 * page.
 *
 * Load-bearing UX element for the "we didn't rebuild n8n" decision: when
 * a user's need outgrows native automations (chains, webhooks, branching),
 * we point them at the orchestrator API as a first-class escape hatch
 * rather than letting them bounce. Three links: API docs (Swagger UI),
 * the external-API guide, and the API Keys page where they mint a PAT.
 */
@Component({
  selector: 'app-escape-hatch-panel',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, TranslocoPipe, AppIconComponent],
  template: `
    <aside class="escape-hatch" aria-labelledby="escape-hatch-title">
      <div class="title-row">
        <app-icon size="inherit" class="title-icon">api</app-icon>
        <h2 id="escape-hatch-title" class="title">
          {{ 'automations.escapeHatch.title' | transloco }}
        </h2>
      </div>
      <p class="body">{{ 'automations.escapeHatch.body' | transloco }}</p>
      <div class="links">
        <a class="link" [href]="apiDocsUrl" target="_blank" rel="noopener">
          <app-icon size="inherit">menu_book</app-icon>
          {{ 'automations.escapeHatch.apiDocs' | transloco }}
        </a>
        <a class="link" [href]="guideUrl" target="_blank" rel="noopener">
          <app-icon size="inherit">description</app-icon>
          {{ 'automations.escapeHatch.guide' | transloco }}
        </a>
        <a class="link" routerLink="/settings/api-keys">
          <app-icon size="inherit">key</app-icon>
          {{ 'automations.escapeHatch.createKey' | transloco }}
        </a>
      </div>
    </aside>
  `,
  styles: [
    `
      .escape-hatch {
        display: flex;
        flex-direction: column;
        gap: var(--space-2, 0.5rem);
        padding: var(--space-4, 1rem);
        background: var(--surface-accent, rgba(64, 96, 192, 0.08));
        border: 1px solid var(--border-accent, rgba(64, 96, 192, 0.3));
        border-radius: var(--radius-surface, 8px);
      }
      .title-row {
        display: flex;
        align-items: center;
        gap: var(--space-2, 0.5rem);
      }
      .title-icon {
        color: var(--text-accent, #4060c0);
      }
      .title {
        margin: 0;
        font-size: 1rem;
        font-weight: 600;
      }
      .body {
        margin: 0;
        opacity: 0.85;
      }
      .links {
        display: flex;
        flex-wrap: wrap;
        gap: var(--space-3, 0.75rem);
        margin-top: var(--space-2, 0.5rem);
      }
      .link {
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
        color: var(--text-accent, #4060c0);
        text-decoration: none;
        font-weight: 500;
      }
      .link:hover,
      .link:focus-visible {
        text-decoration: underline;
      }
    `,
  ],
})
export class EscapeHatchPanelComponent {
  // The orchestrator serves its Swagger UI at /api/docs (create_app sets
  // docs_url). environment.apiUrl includes the /api suffix; strip it to
  // derive the doc URL.
  readonly apiDocsUrl = `${environment.apiUrl.replace(/\/api$/, '')}/api/docs`;
  /** docs/api.md on the public documentation site. */
  readonly guideUrl = API_GUIDE_URL;
}
