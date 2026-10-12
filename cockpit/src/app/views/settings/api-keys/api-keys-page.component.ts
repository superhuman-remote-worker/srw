import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  ViewChild,
  computed,
  inject,
  signal,
} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {ApiKey, ApiKeysService, CreateApiKeyResponse} from '../../../core/services/api-keys.service';
import {UserService} from '../../../core/services/user.service';
import {SidebarToggleComponent} from '../../../shell/sidebar-toggle/sidebar-toggle.component';
import {AppButtonComponent} from '../../../ui/button';
import {AppInputComponent} from '../../../ui/input';
import {AppSelectComponent} from '../../../ui/select';
import {AppCheckboxComponent} from '../../../ui/checkbox';
import {AppIconComponent} from '../../../ui/icon';

/**
 * Personal Access Token (PAT) management.
 *
 * Distinct from the MCP token UI in `settings.component.ts`. PR 3 of
 * knowledge-base/knowledge/features/auth_bff_and_api_tokens.md: PATs are for n8n /
 * automation and use the `ak_` prefix; MCP tokens stay on `srw_` and
 * carry their own scope semantics. Both kinds back onto the same
 * `auth_tokens` table — see the design doc §3.6 for the consolidation
 * rationale.
 *
 * The plaintext token is surfaced exactly once, in a banner at the top
 * of the page after a create or rotate call. The user must acknowledge
 * the warning checkbox to dismiss it.
 */

interface ScopeOption {
  value: string;
  /** Short i18n key under settings.apiKeys.scopes — e.g. `jobsRead`. */
  i18nKey: string;
  /** True if creating with this scope requires the current user to be admin. */
  adminOnly?: boolean;
}

const ALL_SCOPES: ScopeOption[] = [
  {value: 'jobs:read', i18nKey: 'jobsRead'},
  {value: 'jobs:write', i18nKey: 'jobsWrite'},
  {value: 'chat:read', i18nKey: 'chatRead'},
  {value: 'chat:write', i18nKey: 'chatWrite'},
  {value: 'knowledge:read', i18nKey: 'knowledgeRead'},
  {value: 'knowledge:write', i18nKey: 'knowledgeWrite'},
  {value: 'admin', i18nKey: 'admin', adminOnly: true},
];

const DEFAULT_SCOPES = ['jobs:read', 'chat:read'];

@Component({
  selector: 'app-api-keys-page',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    SidebarToggleComponent,
    TranslocoPipe,
    AppButtonComponent,
    AppInputComponent,
    AppSelectComponent,
    AppCheckboxComponent,
    AppIconComponent,
  ],
  template: `
    <div class="api-keys-page">
      <div class="api-keys-container">
        <div class="page-header">
          <app-sidebar-toggle />
          <h1 class="page-title">{{ 'settings.apiKeys.title' | transloco }}</h1>
        </div>

        <p class="page-desc">{{ 'settings.apiKeys.desc' | transloco }}</p>

        <!-- New-token banner (shown after create or rotate). One-shot reveal. -->
        @if (revealedToken(); as rt) {
          <div class="reveal-banner" role="alert">
            <div class="reveal-header">
              <app-icon size="inherit" class="reveal-icon">key</app-icon>
              <strong>{{ 'settings.apiKeys.copyWarning' | transloco }}</strong>
            </div>
            <p class="reveal-detail">{{ 'settings.apiKeys.copyDetail' | transloco }}</p>
            <div class="reveal-row">
              <input
                #tokenInput
                type="text"
                class="reveal-input mono"
                [value]="rt.token"
                readonly
                (focus)="tokenInput.select()"
              />
              <app-button variant="primary" size="md" (clicked)="copyToken(tokenInput)">
                {{ copied() ? ('common.copied' | transloco) : ('common.copy' | transloco) }}
              </app-button>
            </div>
            <label class="reveal-ack">
              <app-checkbox [checked]="acknowledged()" (changed)="acknowledged.set($event)" />
              <span>{{ 'settings.apiKeys.copyAck' | transloco }}</span>
            </label>
            <app-button
              variant="ghost"
              size="sm"
              [disabled]="!acknowledged()"
              (clicked)="dismissReveal()"
            >
              {{ 'settings.apiKeys.copyDone' | transloco }}
            </app-button>
          </div>
        }

        <!-- Existing keys table -->
        <section class="keys-section">
          <h2 class="section-title">{{ 'settings.apiKeys.existingTitle' | transloco }}</h2>
          @if (visibleKeys().length > 0) {
            <div class="keys-table">
              <div class="keys-header">
                <span class="col-name">{{ 'settings.apiKeys.colName' | transloco }}</span>
                <span class="col-scopes">{{ 'settings.apiKeys.colScopes' | transloco }}</span>
                <span class="col-hint">{{ 'settings.apiKeys.colHint' | transloco }}</span>
                <span class="col-used">{{ 'settings.apiKeys.colLastUsed' | transloco }}</span>
                <span class="col-expires">{{ 'settings.apiKeys.colExpires' | transloco }}</span>
                <span class="col-action"></span>
              </div>
              @for (key of visibleKeys(); track key.id) {
                <div class="keys-row" [class.stale]="isStale(key)" [class.superseded]="!!key.superseded_by">
                  <span class="col-name">
                    {{ key.name }}
                    @if (key.superseded_by) {
                      <span class="rot-badge">{{ 'settings.apiKeys.rotatedBadge' | transloco }}</span>
                    }
                  </span>
                  <span class="col-scopes">
                    @for (s of key.scopes; track s) {
                      <span class="scope-chip">{{ s }}</span>
                    }
                  </span>
                  <span class="col-hint mono">{{ formatHint(key) }}</span>
                  <span class="col-used">{{ key.last_used_at ? formatDate(key.last_used_at) : ('common.never' | transloco) }}</span>
                  <span class="col-expires">{{ key.expires_at ? formatDate(key.expires_at) : ('common.never' | transloco) }}</span>
                  <span class="col-action">
                    @if (!key.superseded_by) {
                      <app-button variant="ghost" size="sm" (clicked)="rotate(key)">
                        {{ 'settings.apiKeys.rotate' | transloco }}
                      </app-button>
                    }
                    <app-button variant="danger" size="sm" (clicked)="revoke(key)">
                      {{ 'settings.apiKeys.revoke' | transloco }}
                    </app-button>
                  </span>
                </div>
              }
            </div>
          } @else if (!service.isLoading()) {
            <p class="empty-state">{{ 'settings.apiKeys.empty' | transloco }}</p>
          }
        </section>

        <!-- Create form -->
        <section class="create-section">
          <h2 class="section-title">{{ 'settings.apiKeys.createTitle' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.apiKeys.createDesc' | transloco }}</p>

          @if (createError(); as err) {
            <div class="create-error">{{ err }}</div>
          }

          <div class="create-form">
            <div class="form-row">
              <app-input
                [value]="formName()"
                [placeholder]="'settings.apiKeys.namePlaceholder' | transloco"
                [disabled]="creating()"
                (changed)="formName.set($event)"
              />
            </div>

            <fieldset class="scopes-fieldset">
              <legend>{{ 'settings.apiKeys.scopesLegend' | transloco }}</legend>
              <div class="scopes-grid">
                @for (s of availableScopes(); track s.value) {
                  <label class="scope-checkbox">
                    <app-checkbox
                      [checked]="formScopes().has(s.value)"
                      (changed)="toggleScope(s.value, $event)"
                    />
                    <span>
                      <strong>{{ s.value }}</strong>
                      <small>{{ ('settings.apiKeys.scopes.' + s.i18nKey) | transloco }}</small>
                    </span>
                  </label>
                }
              </div>
            </fieldset>

            <div class="form-row">
              <label class="form-label">{{ 'settings.apiKeys.expiryLabel' | transloco }}</label>
              <app-select
                [value]="formExpiryText()"
                [disabled]="creating()"
                (changed)="setExpiry($event)"
              >
                <option value="30">{{ 'settings.mcp.expiry30' | transloco }}</option>
                <option value="90">{{ 'settings.mcp.expiry90' | transloco }}</option>
                <option value="365">{{ 'settings.mcp.expiry365' | transloco }}</option>
                <option value="">{{ 'settings.apiKeys.expiryNever' | transloco }}</option>
              </app-select>
            </div>

            <app-button
              variant="primary"
              size="md"
              [loading]="creating()"
              [disabled]="creating() || !canCreate()"
              (clicked)="create()"
            >
              {{ creating() ? ('settings.apiKeys.creating' | transloco) : ('settings.apiKeys.create' | transloco) }}
            </app-button>
          </div>
        </section>

        <!-- n8n example -->
        <section class="hint-section">
          <h2 class="section-title">{{ 'settings.apiKeys.usageTitle' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.apiKeys.usageDesc' | transloco }}</p>
          <pre class="code-block">{{ usageSnippet }}</pre>
        </section>
      </div>
    </div>
  `,
  styles: [`
    /* overflow: auto matches the parent settings page. Without it the host is a
       600px-tall flex item holding ~850px of content and the shell's
       .content-area clips the rest: "Create token" and the whole "How to use"
       section sat below the fold with no scrollbar anywhere on a short viewport. */
    :host { display: block; height: 100%; overflow: auto; }
    .api-keys-page { padding: 24px; max-width: var(--content-max-width); margin: 0 auto; }
    .api-keys-container { display: flex; flex-direction: column; gap: 24px; }
    .page-header { display: flex; align-items: center; gap: 12px; }
    .page-title { margin: 0; font-size: 1.5rem; }
    .page-desc { color: var(--text-secondary); margin: 0; }
    .section-title { margin: 0 0 4px; font-size: 1.1rem; }
    .section-desc { color: var(--text-secondary); margin: 0 0 12px; font-size: 0.9rem; }
    .empty-state { color: var(--text-muted); font-style: italic; }
    .keys-section, .create-section, .hint-section { background: var(--panel-bg); border: 1px solid var(--border-hairline); border-radius: var(--radius-surface); padding: 16px 20px; }

    .keys-table { display: flex; flex-direction: column; gap: 4px; }
    .keys-header, .keys-row {
      display: grid;
      grid-template-columns: 1.5fr 1.6fr 1fr 1fr 1fr auto;
      gap: 12px;
      align-items: center;
      padding: 8px 4px;
    }
    .keys-header { font-weight: 600; font-size: 0.85rem; color: var(--text-secondary); border-bottom: 1px solid var(--border-color); }
    .keys-row { border-bottom: 1px solid var(--border-color); }
    .keys-row.stale { background: color-mix(in srgb, var(--warning) 8%, transparent); }
    .keys-row.superseded { opacity: 0.6; }
    .col-action { display: flex; gap: 6px; justify-content: flex-end; }
    .col-scopes { display: flex; flex-wrap: wrap; gap: 4px; }
    .scope-chip {
      font-size: 0.7rem;
      padding: 2px 6px;
      border-radius: var(--radius-tag);
      background: var(--surface-2);
      color: var(--text-secondary);
      font-family: var(--font-mono);
    }
    .rot-badge {
      font-size: 0.7rem;
      padding: 2px 6px;
      border-radius: var(--radius-tag);
      background: color-mix(in srgb, var(--warning) 30%, transparent);
      color: var(--warning);
      margin-left: 6px;
    }
    .mono { font-family: var(--font-mono); font-size: 0.85rem; }

    .reveal-banner {
      background: color-mix(in srgb, var(--warning) 10%, var(--surface-1));
      border: 1px solid var(--warning);
      border-radius: var(--radius-surface);
      padding: 16px 20px;
      display: flex; flex-direction: column; gap: 8px;
    }
    .reveal-header { display: flex; align-items: center; gap: 8px; }
    .reveal-detail { color: var(--text-secondary); margin: 0; }
    .reveal-row { display: flex; gap: 8px; align-items: center; }
    .reveal-input { flex: 1; padding: 8px 12px; border: 1px solid var(--border-color); border-radius: var(--radius-control); background: var(--surface-2); color: var(--text-primary); }
    .reveal-ack { display: flex; align-items: center; gap: 8px; color: var(--text-secondary); }

    .create-form { display: flex; flex-direction: column; gap: 12px; }
    .form-row { display: flex; flex-direction: column; gap: 4px; }
    .form-label { font-weight: 500; font-size: 0.9rem; color: var(--text-secondary); }
    /* Reset the UA fieldset box so the scope grid lines up with the sibling fields. */
    .scopes-fieldset { border: 0; padding: 0; margin: 0; min-width: 0; }
    .scopes-fieldset legend { padding: 0; margin-bottom: 8px; font-weight: 600; font-size: 0.9rem; }
    .scopes-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 8px; }
    .scope-checkbox { display: flex; gap: 8px; align-items: flex-start; cursor: pointer; }
    .scope-checkbox small { display: block; color: var(--text-secondary); font-size: 0.8rem; }
    .create-error { color: var(--danger); padding: 8px 12px; border-radius: var(--radius-control); background: color-mix(in srgb, var(--danger) 12%, transparent); }

    .code-block { background: var(--surface-0); padding: 12px; border-radius: var(--radius-control); overflow-x: auto; font-family: var(--font-mono); font-size: 0.85rem; }
  `],
})
export class ApiKeysPageComponent {
  readonly service = inject(ApiKeysService);
  private readonly userService = inject(UserService);

  /** Master list of all scope options. */
  readonly scopes = ALL_SCOPES;

  readonly availableScopes = computed(() => {
    const isAdmin = this.userService.currentUser()?.is_admin === true;
    return ALL_SCOPES.filter((s) => isAdmin || !s.adminOnly);
  });

  readonly visibleKeys = computed(() =>
    [...this.service.keys()]
      // Sort: active > superseded; within group, recently-used first; then created.
      .sort((a, b) => {
        const ar = a.revoked_at ? 1 : 0;
        const br = b.revoked_at ? 1 : 0;
        if (ar !== br) return ar - br;
        const at = a.last_used_at ?? a.created_at;
        const bt = b.last_used_at ?? b.created_at;
        return bt.localeCompare(at);
      })
      .filter((k) => !k.revoked_at),
  );

  // ── form state ──
  readonly formName = signal('');
  readonly formScopes = signal<Set<string>>(new Set(DEFAULT_SCOPES));
  readonly formExpiryText = signal<string>('365');
  readonly canCreate = computed(() => this.formName().trim().length > 0 && this.formScopes().size > 0);

  readonly creating = signal(false);
  readonly createError = signal<string | null>(null);

  // ── reveal state ──
  readonly revealedToken = signal<CreateApiKeyResponse | null>(null);
  readonly acknowledged = signal(false);
  readonly copied = signal(false);

  /** A short curl example users can paste into n8n's Header Auth credential. */
  readonly usageSnippet =
    `curl -H "Authorization: Bearer ak_<your_token>" \\\n` +
    `     ${origin().replace(/\/$/, '')}/api/jobs`;

  constructor() {
    this.service.loadKeys();
  }

  toggleScope(value: string, checked: boolean): void {
    this.formScopes.update((s) => {
      const next = new Set(s);
      if (checked) next.add(value);
      else next.delete(value);
      return next;
    });
  }

  setExpiry(value: string | null | undefined): void {
    this.formExpiryText.set(value ?? '365');
  }

  create(): void {
    if (!this.canCreate() || this.creating()) return;
    this.creating.set(true);
    this.createError.set(null);
    const expiry = this.formExpiryText();
    const expiresInDays = expiry === '' ? null : Number(expiry);
    this.service
      .createKey({
        name: this.formName().trim(),
        scopes: Array.from(this.formScopes()).sort(),
        expires_in_days: expiresInDays,
      })
      .subscribe({
        next: (created) => {
          this.creating.set(false);
          this.revealedToken.set(created);
          this.acknowledged.set(false);
          this.copied.set(false);
          this.formName.set('');
          this.formScopes.set(new Set(DEFAULT_SCOPES));
          this.formExpiryText.set('365');
        },
        error: (err) => {
          this.creating.set(false);
          const detail =
            (err?.error?.detail as string | undefined) ?? 'Create failed';
          this.createError.set(detail);
        },
      });
  }

  rotate(key: ApiKey): void {
    if (!window.confirm(`Rotate the token "${key.name}"? The old token stays valid for 24 hours so automations can roll over.`)) return;
    this.service.rotateKey(key.id).subscribe({
      next: (rotated) => {
        this.revealedToken.set(rotated);
        this.acknowledged.set(false);
        this.copied.set(false);
      },
    });
  }

  revoke(key: ApiKey): void {
    if (!window.confirm(`Revoke the token "${key.name}"? Anything using it will start failing immediately.`)) return;
    this.service.revokeKey(key.id).subscribe();
  }

  copyToken(input: HTMLInputElement): void {
    input.select();
    void navigator.clipboard.writeText(input.value).then(() => {
      this.copied.set(true);
      setTimeout(() => this.copied.set(false), 2000);
    });
  }

  dismissReveal(): void {
    if (!this.acknowledged()) return;
    this.revealedToken.set(null);
    this.acknowledged.set(false);
  }

  formatHint(key: ApiKey): string {
    const tail = key.last_four ? `…${key.last_four}` : '…';
    return `${key.token_prefix}${tail}`;
  }

  formatDate(iso: string): string {
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
  }

  /** Highlights tokens that haven't been used in 90+ days. */
  isStale(key: ApiKey): boolean {
    if (!key.last_used_at) {
      // Never used. Only stale if it's been sitting around for 90+ days.
      const created = Date.parse(key.created_at);
      return Number.isFinite(created) && Date.now() - created > 90 * 24 * 3600 * 1000;
    }
    const last = Date.parse(key.last_used_at);
    return Number.isFinite(last) && Date.now() - last > 90 * 24 * 3600 * 1000;
  }
}

function origin(): string {
  if (typeof window === 'undefined') return '';
  // Prefer the API origin (cookie BFF host). Falls back to current origin
  // when running in dev without a configured apiUrl.
  const env = (window as unknown as { env?: { apiUrl?: string } }).env;
  const apiUrl = env?.apiUrl ?? '';
  return apiUrl.replace(/\/api\/?$/, '') || window.location.origin;
}
