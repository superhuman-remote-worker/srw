import {ChangeDetectionStrategy, Component, computed, inject, OnInit, output, signal} from '@angular/core';
import {RouterLink} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {
  ApiKeyProvider,
  CATALOG_CAPABILITIES,
  CatalogCapability,
  DiscoveryCandidate,
  DiscoveryResponse,
  LlmEndpoint,
  LlmEndpointTestResult,
  LlmEndpointUpdateRequest,
} from '../../../core/models/api.model';
import {timer} from 'rxjs';
import {filter, switchMap, take} from 'rxjs/operators';
import {AppButtonComponent} from '../../../ui/button';
import {AppInputComponent} from '../../../ui/input';
import {AppSelectComponent} from '../../../ui/select';
import {AppCheckboxComponent} from '../../../ui/checkbox';
import {AppBadgeComponent} from '../../../ui/badge';
import {HelmManagedBadgeComponent} from '../../../ui/helm-managed-badge/helm-managed-badge.component';
import {AppFormFieldComponent} from '../../../ui/form-field';

/** Providers the admin can seed/rotate — matches VALID_SYSTEM_API_KEY_PROVIDERS. */
const SYSTEM_PROVIDERS: {value: Exclude<ApiKeyProvider, 'codex'>; label: string}[] = [
  {value: 'openai', label: 'OpenAI'},
  {value: 'anthropic', label: 'Anthropic'},
  {value: 'google', label: 'Google'},
  {value: 'groq', label: 'Groq'},
  {value: 'openrouter', label: 'OpenRouter'},
  {value: 'mistral', label: 'Mistral'},
  {value: 'vision', label: 'Vision'},
];

type SystemProviderValue = (typeof SYSTEM_PROVIDERS)[number]['value'];

// Providers we know how to enumerate via the orchestrator's discovery
// service. Keep in sync with `discovery_service.DISCOVERABLE_PROVIDERS`.
const DISCOVERABLE_PROVIDERS: ReadonlySet<string> = new Set([
  'openai',
  'google',
  'groq',
  'openrouter',
  'mistral',
]);

@Component({
  selector: 'app-admin-providers',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    RouterLink,
    TranslocoPipe,
    HelmManagedBadgeComponent,
    AppButtonComponent,
    AppInputComponent,
    AppSelectComponent,
    AppCheckboxComponent,
    AppBadgeComponent,
    AppFormFieldComponent,
  ],
  template: `
    <div class="admin-providers">
      <!-- Provider Keys -->
        <section class="admin-section">
          <h2 class="section-title">{{ 'admin.providers.keys.title' | transloco }}</h2>
          <p class="section-desc">{{ 'admin.providers.keys.desc' | transloco }}</p>

          @if (admin.systemApiKeys().length > 0) {
            <div class="key-table">
              <div class="key-header">
                <span class="col-provider">{{ 'admin.providers.keys.colProvider' | transloco }}</span>
                <span class="col-prefix">{{ 'admin.providers.keys.colKey' | transloco }}</span>
                <span class="col-label">{{ 'admin.providers.keys.colLabel' | transloco }}</span>
                <span class="col-source">{{ 'admin.providers.keys.colSource' | transloco }}</span>
                <span class="col-updated">{{ 'admin.providers.keys.colUpdated' | transloco }}</span>
                <span class="col-action"></span>
              </div>
              @for (key of admin.systemApiKeys(); track key.id) {
                <div class="key-row">
                  <span class="col-provider">{{ providerLabel(key.provider) }}</span>
                  <span class="col-prefix mono">{{ key.key_prefix }}...</span>
                  <span class="col-label">{{ key.label || '-' }}</span>
                  <span class="col-source">
                    @if (key.seeded_from) {
                      <app-badge
                        tone="neutral"
                        size="sm"
                        [title]="key.seeded_from"
                      >
                        {{ 'admin.providers.keys.seededBadge' | transloco }}
                      </app-badge>
                    } @else {
                      <span class="muted">{{ 'admin.providers.keys.manualBadge' | transloco }}</span>
                    }
                    <app-helm-managed-badge [managed]="key.managed_by_helm" [drift]="key.helm_drift" />
                  </span>
                  <span class="col-updated">{{ formatDate(key.updated_at) }}</span>
                  <span class="col-action">
                    <app-button
                      variant="danger"
                      size="sm"
                      (clicked)="deleteKey(key.provider)"
                    >
                      {{ 'common.delete' | transloco }}
                    </app-button>
                  </span>
                </div>
              }
            </div>
          } @else {
            <p class="empty-state">{{ 'admin.providers.keys.empty' | transloco }}</p>
          }

          <div class="create-form">
            <h3 class="form-title">{{ 'admin.providers.keys.addTitle' | transloco }}</h3>
            <div class="form-row two-col">
              <app-form-field>
                <app-select
                  [value]="keyProvider()"
                  [disabled]="savingKey()"
                  (changed)="onKeyProviderChange($event)"
                >
                  @for (p of providers; track p.value) {
                    <option [value]="p.value">{{ p.label }}</option>
                  }
                </app-select>
              </app-form-field>
              <app-form-field>
                <app-input
                  [value]="keyLabel()"
                  [placeholder]="'admin.providers.keys.labelPlaceholder' | transloco"
                  [disabled]="savingKey()"
                  (changed)="keyLabel.set($event)"
                />
              </app-form-field>
            </div>
            <div class="form-row">
              <app-form-field>
                <app-input
                  type="password"
                  [value]="keyValue()"
                  [placeholder]="'admin.providers.keys.keyPlaceholder' | transloco"
                  [disabled]="savingKey()"
                  (changed)="keyValue.set($event)"
                />
              </app-form-field>
            </div>
            @if (keyFormError()) {
              <p class="form-error">{{ keyFormError() }}</p>
            }
            <app-button
              variant="primary"
              size="md"
              [loading]="savingKey()"
              [disabled]="savingKey() || !keyValue().trim()"
              (clicked)="saveKey()"
            >
              {{ savingKey() ? ('common.saving' | transloco) : ('admin.providers.keys.saveButton' | transloco) }}
            </app-button>
          </div>

          <!-- Auto-discovery confirmation dialog (inline; opens after a
               successful key save for openai/google/groq/openrouter) -->
          @if (discoveryProvider(); as discProvider) {
            <div class="discovery-pane">
              <div class="discovery-head">
                <strong>
                  Models discovered for {{ providerLabel(discProvider) }}
                </strong>
                <div class="discovery-actions">
                  <app-button
                    variant="secondary"
                    size="sm"
                    [loading]="discoveryPolling()"
                    [disabled]="discoveryPolling() || discoveryAdding()"
                    (clicked)="rediscover()"
                  >
                    Rediscover
                  </app-button>
                  <app-button
                    variant="ghost"
                    size="sm"
                    [disabled]="discoveryAdding()"
                    (clicked)="closeDiscovery()"
                  >
                    Close
                  </app-button>
                </div>
              </div>

              @if (discoveryPolling() && !discoveryPayload()) {
                <p class="muted">
                  Probing the provider's model listing… (this is non-blocking;
                  the key is already saved.)
                </p>
              } @else if (discoveryPayload(); as res) {
                @if (!res.payload || res.payload.count === 0) {
                  <p class="empty-state">
                    The provider returned no models — your key may lack the
                    necessary scope, or the listing endpoint is unavailable.
                    Add models manually via Admin → Models.
                  </p>
                } @else {
                  <p class="muted">
                    Pick which models become catalog rows. Supported families
                    have custom prompts and settings; generic ones work but
                    use defaults.
                  </p>

                  @if (supportedCandidates().length > 0) {
                    <div class="discovery-tier">
                      <h4 class="discovery-tier-title">
                        Supported ({{ supportedCandidates().length }})
                      </h4>
                      @for (c of supportedCandidates(); track c.model_id) {
                        <div class="discovery-row">
                          <app-checkbox
                            size="sm"
                            [checked]="discoverySelections()[c.model_id]?.selected ?? false"
                            [ariaLabel]="c.model_id"
                            (changed)="toggleCandidate(c.model_id, $event)"
                          />
                          <span class="discovery-id mono">{{ c.model_id }}</span>
                          <span class="discovery-family">{{ c.detected_family }}</span>
                          <div class="discovery-caps">
                            @for (cap of capabilities; track cap) {
                              <label class="discovery-cap-chip">
                                <app-checkbox
                                  size="sm"
                                  [checked]="candidateHasCapability(c.model_id, cap)"
                                  [ariaLabel]="c.model_id + ':' + cap"
                                  (changed)="toggleCandidateCapability(c.model_id, cap, $event)"
                                />
                                <span>{{ cap }}</span>
                              </label>
                            }
                          </div>
                        </div>
                      }
                    </div>
                  }

                  @if (genericCandidates().length > 0) {
                    <div class="discovery-tier">
                      <h4 class="discovery-tier-title">
                        Generic ({{ genericCandidates().length }})
                        <button
                          type="button"
                          class="link-button"
                          (click)="discoveryShowGeneric.set(!discoveryShowGeneric())"
                        >
                          {{ discoveryShowGeneric() ? 'Hide' : 'Show models without custom prompts' }}
                        </button>
                      </h4>
                      @if (discoveryShowGeneric()) {
                        @for (c of genericCandidates(); track c.model_id) {
                          <div class="discovery-row">
                            <app-checkbox
                              size="sm"
                              [checked]="discoverySelections()[c.model_id]?.selected ?? false"
                              [ariaLabel]="c.model_id"
                              (changed)="toggleCandidate(c.model_id, $event)"
                            />
                            <span class="discovery-id mono">{{ c.model_id }}</span>
                            <span class="discovery-family muted">default</span>
                            <div class="discovery-caps">
                              @for (cap of capabilities; track cap) {
                                <label class="discovery-cap-chip">
                                  <app-checkbox
                                    size="sm"
                                    [checked]="candidateHasCapability(c.model_id, cap)"
                                    [ariaLabel]="c.model_id + ':' + cap"
                                    (changed)="toggleCandidateCapability(c.model_id, cap, $event)"
                                  />
                                  <span>{{ cap }}</span>
                                </label>
                              }
                            </div>
                          </div>
                        }
                      }
                    </div>
                  }

                  <div class="discovery-footer">
                    <app-button
                      variant="primary"
                      size="md"
                      [loading]="discoveryAdding()"
                      [disabled]="selectedCount() === 0 || discoveryAdding()"
                      (clicked)="addSelectedCandidates()"
                    >
                      Add {{ selectedCount() }} selected
                    </app-button>
                  </div>
                }
              }

              @if (discoveryError()) {
                <p class="form-error">{{ discoveryError() }}</p>
              }
            </div>
          }
        </section>

        <!-- System Endpoints -->
        <section class="admin-section section-spacer">
          <h2 class="section-title">{{ 'admin.providers.endpoints.title' | transloco }}</h2>
          <p class="section-desc">{{ 'admin.providers.endpoints.desc' | transloco }}</p>

          @if (admin.systemEndpoints().length > 0) {
            @for (endpoint of admin.systemEndpoints(); track endpoint.id) {
              <div class="endpoint-card">
                <div class="endpoint-head">
                  <div class="endpoint-title">
                    <strong>{{ endpoint.label }}</strong>
                    <app-helm-managed-badge [managed]="endpoint.managed_by_helm" [drift]="endpoint.helm_drift" />
                    @if (isCodexEndpoint(endpoint.label)) {
                      <app-badge tone="info" size="sm" [uppercase]="true" shape="pill">
                        codex subscription
                      </app-badge>
                    }
                    <span class="endpoint-url mono">{{ endpoint.base_url }}</span>
                    @if (endpoint.key_prefix) {
                      <span class="endpoint-key mono">key {{ endpoint.key_prefix }}...</span>
                    } @else {
                      <span class="endpoint-key muted">{{ 'admin.providers.endpoints.noKey' | transloco }}</span>
                    }
                  </div>
                  <div class="endpoint-actions">
                    <app-button
                      variant="secondary"
                      size="sm"
                      [loading]="testingEndpointId() === endpoint.id"
                      [disabled]="testingEndpointId() === endpoint.id"
                      (clicked)="testEndpoint(endpoint.id)"
                    >
                      {{ testingEndpointId() === endpoint.id
                          ? ('admin.providers.endpoints.testing' | transloco)
                          : ('admin.providers.endpoints.testButton' | transloco) }}
                    </app-button>
                    <app-button
                      variant="secondary"
                      size="sm"
                      (clicked)="discoverEndpoint(endpoint.id)"
                    >
                      Discover
                    </app-button>
                    @if (!isCodexEndpoint(endpoint.label)) {
                      <app-button
                        variant="secondary"
                        size="sm"
                        [disabled]="editingEndpointId() === endpoint.id"
                        (clicked)="startEditEndpoint(endpoint)"
                      >
                        {{ 'common.edit' | transloco }}
                      </app-button>
                    }
                    <app-button
                      variant="danger"
                      size="sm"
                      (clicked)="deleteEndpoint(endpoint.id)"
                    >
                      {{ 'common.delete' | transloco }}
                    </app-button>
                  </div>
                </div>

                @if (testResults()[endpoint.id]; as result) {
                  <div
                    class="test-result"
                    [class.ok]="result.ok"
                    [class.err]="!result.ok"
                  >
                    @if (result.ok) {
                      {{ 'admin.providers.endpoints.testOk' | transloco: {status: result.status} }}
                    } @else {
                      {{ 'admin.providers.endpoints.testFail' | transloco:
                        {status: result.status ?? '-',
                         error: result.error ?? ''} }}
                    }
                  </div>
                }

                @if (editingEndpointId() === endpoint.id) {
                  <div class="edit-form">
                    <h4 class="form-title">
                      {{ 'admin.providers.endpoints.editTitle' | transloco }}
                    </h4>
                    <div class="form-row two-col">
                      <app-form-field>
                        <app-input
                          [value]="editEndpointLabel()"
                          [placeholder]="'admin.providers.endpoints.labelPlaceholder' | transloco"
                          [disabled]="savingEdit()"
                          (changed)="editEndpointLabel.set($event)"
                        />
                      </app-form-field>
                      <app-form-field>
                        <app-input
                          [value]="editEndpointBaseUrl()"
                          [placeholder]="'admin.providers.endpoints.baseUrlPlaceholder' | transloco"
                          [disabled]="savingEdit()"
                          (changed)="editEndpointBaseUrl.set($event)"
                        />
                      </app-form-field>
                    </div>
                    <div class="form-row">
                      <app-form-field>
                        <app-input
                          type="password"
                          [value]="editEndpointApiKey()"
                          [placeholder]="'admin.providers.endpoints.apiKeyPlaceholderEdit' | transloco"
                          [disabled]="savingEdit() || editEndpointClearKey()"
                          (changed)="editEndpointApiKey.set($event)"
                        />
                      </app-form-field>
                    </div>
                    <app-checkbox
                      size="sm"
                      [checked]="editEndpointClearKey()"
                      [disabled]="savingEdit() || !!editEndpointApiKey().trim()"
                      (changed)="editEndpointClearKey.set($event)"
                    >
                      {{ 'admin.providers.endpoints.removeKey' | transloco }}
                    </app-checkbox>
                    <app-checkbox
                      size="sm"
                      [checked]="editEndpointAllowInsecure()"
                      [disabled]="savingEdit()"
                      (changed)="editEndpointAllowInsecure.set($event)"
                    >
                      {{ 'admin.providers.endpoints.allowInsecure' | transloco }}
                    </app-checkbox>
                    @if (editFormError()) {
                      <p class="form-error">{{ editFormError() }}</p>
                    }
                    <div class="edit-form-actions">
                      <app-button
                        variant="primary"
                        size="md"
                        [loading]="savingEdit()"
                        [disabled]="savingEdit() || !editEndpointLabel().trim() || !editEndpointBaseUrl().trim()"
                        (clicked)="saveEditEndpoint()"
                      >
                        {{ savingEdit()
                            ? ('common.saving' | transloco)
                            : ('common.save' | transloco) }}
                      </app-button>
                      <app-button
                        variant="ghost"
                        size="md"
                        [disabled]="savingEdit()"
                        (clicked)="cancelEditEndpoint()"
                      >
                        {{ 'common.cancel' | transloco }}
                      </app-button>
                    </div>
                  </div>
                }

                <p class="catalog-hint">
                  {{ 'admin.providers.endpoints.catalogHint' | transloco }}
                  <button
                    type="button"
                    class="catalog-hint-link catalog-hint-button"
                    (click)="switchTab.emit('models')"
                  >
                    {{ 'admin.providers.endpoints.catalogHintLink' | transloco }}
                  </button>
                  @if (isCodexEndpoint(endpoint.label)) {
                    <span class="codex-aside">
                      ·
                      <a routerLink="/admin/subscriptions" class="catalog-hint-link">
                        Manage subscriptions
                      </a>
                    </span>
                  }
                </p>
              </div>
            }
          } @else {
            <p class="empty-state">{{ 'admin.providers.endpoints.empty' | transloco }}</p>
          }

          <div class="create-form">
            <h3 class="form-title">{{ 'admin.providers.endpoints.addTitle' | transloco }}</h3>
            <div class="form-row two-col">
              <app-form-field>
                <app-input
                  [value]="newEndpointLabel()"
                  [placeholder]="'admin.providers.endpoints.labelPlaceholder' | transloco"
                  [disabled]="creatingEndpoint()"
                  (changed)="newEndpointLabel.set($event)"
                />
              </app-form-field>
              <app-form-field>
                <app-input
                  [value]="newEndpointBaseUrl()"
                  [placeholder]="'admin.providers.endpoints.baseUrlPlaceholder' | transloco"
                  [disabled]="creatingEndpoint()"
                  (changed)="newEndpointBaseUrl.set($event)"
                />
              </app-form-field>
            </div>
            <div class="form-row">
              <app-form-field>
                <app-input
                  type="password"
                  [value]="newEndpointApiKey()"
                  [placeholder]="'admin.providers.endpoints.apiKeyPlaceholder' | transloco"
                  [disabled]="creatingEndpoint()"
                  (changed)="newEndpointApiKey.set($event)"
                />
              </app-form-field>
            </div>
            <app-checkbox
              size="sm"
              [checked]="newEndpointAllowInsecure()"
              [disabled]="creatingEndpoint()"
              (changed)="newEndpointAllowInsecure.set($event)"
            >
              {{ 'admin.providers.endpoints.allowInsecure' | transloco }}
            </app-checkbox>
            @if (endpointFormError()) {
              <p class="form-error">{{ endpointFormError() }}</p>
            }
            <app-button
              variant="primary"
              size="md"
              [loading]="creatingEndpoint()"
              [disabled]="creatingEndpoint() || !newEndpointLabel().trim() || !newEndpointBaseUrl().trim()"
              (clicked)="createEndpoint()"
            >
              {{ creatingEndpoint()
                  ? ('common.saving' | transloco)
                  : ('admin.providers.endpoints.createButton' | transloco) }}
            </app-button>
          </div>
        </section>
    </div>
  `,
  styles: [`
    :host {
      display: block;
    }
    .admin-providers {
      display: block;
    }
    .admin-section {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 24px;
    }
    .section-spacer { margin-top: 24px; }
    .section-title {
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 4px;
      color: var(--text-primary);
    }
    .section-desc {
      font-size: 13px;
      color: var(--text-muted);
      margin-bottom: 20px;
    }
    .key-table {
      margin-bottom: 20px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      overflow: hidden;
    }
    .key-header,
    .key-row {
      display: grid;
      grid-template-columns: 1.3fr 1fr 1fr 0.9fr 0.9fr 90px;
      padding: 10px 14px;
      gap: 8px;
      align-items: center;
      font-size: 13px;
    }
    .key-header {
      background: var(--surface-0);
      font-weight: 600;
      font-size: 12px;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .key-row {
      border-top: 1px solid var(--border-color);
    }
    .mono {
      font-family: 'JetBrains Mono', 'Fira Code', monospace;
      font-size: 12px;
      color: var(--text-muted);
    }
    .muted { color: var(--text-muted); }
    .test-result {
      margin: 8px 0;
      padding: 8px 12px;
      border-radius: var(--radius-control);
      font-size: 12px;
    }
    .test-result.ok {
      background: var(--success-tint);
      color: var(--success);
    }
    .test-result.err {
      background: var(--danger-tint);
      color: var(--danger);
    }
    .empty-state {
      font-size: 13px;
      color: var(--text-muted);
      text-align: center;
      padding: 18px 12px;
    }
    .endpoint-card {
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 14px;
      margin-bottom: 12px;
    }
    .endpoint-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .endpoint-title {
      display: flex;
      flex-direction: column;
      gap: 2px;
      font-size: 13px;
    }
    .endpoint-url,
    .endpoint-key {
      font-size: 11px;
    }
    .endpoint-actions {
      display: flex;
      gap: 6px;
      align-items: center;
    }
    .catalog-hint {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px dashed var(--border-color);
      font-size: 12px;
      color: var(--text-muted);
    }
    .catalog-hint-link {
      color: var(--accent-color);
      text-decoration: none;
      margin-left: 4px;
    }
    .catalog-hint-link:hover { text-decoration: underline; }
    .catalog-hint-button {
      background: none;
      border: 0;
      padding: 0;
      font: inherit;
      cursor: pointer;
    }
    .codex-aside { margin-left: 6px; }
    .create-form {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px dashed var(--border-color);
    }
    .edit-form {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px dashed var(--border-color);
    }
    .edit-form-actions {
      display: flex;
      gap: 8px;
      margin-top: 8px;
    }
    .form-row {
      display: flex;
      gap: 8px;
      margin-bottom: 8px;
    }
    .form-row.two-col > * { flex: 1; }
    .form-title {
      font-size: 14px;
      font-weight: 600;
      margin: 0 0 12px 0;
      color: var(--text-primary);
    }
    .form-error {
      color: var(--danger);
      font-size: 12px;
      margin: 4px 0 8px 0;
    }
    .mono { font-family: ui-monospace, monospace; font-size: 12px; }
    .muted { color: var(--text-muted); }
    .discovery-pane {
      margin-top: 16px;
      padding: 14px;
      background: var(--surface-0);
      border: 1px dashed var(--border-color);
      border-radius: var(--radius-surface);
    }
    .discovery-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 10px;
    }
    .discovery-actions {
      display: flex;
      gap: 6px;
    }
    .discovery-tier {
      margin-top: 10px;
    }
    .discovery-tier-title {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
      margin: 8px 0 6px 0;
      display: flex;
      gap: 12px;
      align-items: center;
    }
    .discovery-row {
      display: grid;
      grid-template-columns: 24px 1.6fr 0.8fr 1.6fr;
      gap: 8px;
      align-items: center;
      padding: 4px 0;
      font-size: 13px;
    }
    .discovery-family {
      font-size: 11px;
    }
    .discovery-caps {
      display: flex;
      flex-wrap: wrap;
      gap: 6px 10px;
    }
    .discovery-cap-chip {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      font-size: 11px;
      color: var(--text-secondary);
      cursor: pointer;
      user-select: none;
    }
    .discovery-footer {
      margin-top: 10px;
      display: flex;
      justify-content: flex-end;
    }
    .link-button {
      background: none;
      border: 0;
      padding: 0;
      cursor: pointer;
      color: var(--accent-color);
      font-size: 11px;
      text-transform: none;
      letter-spacing: 0;
    }
    .link-button:hover { text-decoration: underline; }
    @media (max-width: 720px) {
      /* Provider API Keys table is a 6-column grid
         (1.3fr 1fr 1fr 0.9fr 0.9fr 90px); at phone widths six columns squish
         into ~318px (~40px each), truncating every field. Collapse each row
         into a stacked card. Desktop (>720px) keeps the table grid. */
      .key-header {
        display: none;
      }
      .key-row {
        grid-template-columns: 1fr;
        gap: 4px;
        padding: 12px 14px;
      }
      .key-row .col-provider {
        font-size: 14px;
        font-weight: 600;
      }
      .key-row .col-updated::before {
        content: 'Updated: ';
        color: var(--text-muted);
      }
      .key-row .col-action {
        margin-top: 4px;
      }
      /* System Endpoint cards: the 4-button action row (Test / Discover /
         Edit / Delete) runs Delete off the right edge. Stack the head so the
         actions get their own full-width line and wrap. */
      .endpoint-head {
        flex-direction: column;
        align-items: stretch;
      }
      .endpoint-actions {
        flex-wrap: wrap;
      }
      /* Stack the cramped two-up form rows. */
      .form-row.two-col {
        flex-direction: column;
      }
      /* Discovery dialog rows: the 4-col grid (24px 1.6fr 0.8fr 1.6fr) crams
         model-id + family + capability checkboxes into ~40-80px columns at
         phone widths (id wraps to 3 lines, caps stack one-per-line). Re-flow to
         a checkbox + stacked-content layout: id / family / capabilities each
         span the full content width. The direct-child combinator keeps the
         per-capability checkboxes (nested in .discovery-caps) untouched. */
      .discovery-row {
        grid-template-columns: 24px 1fr;
        align-items: start;
        gap: 4px 8px;
        padding: 8px 0;
      }
      .discovery-row > app-checkbox {
        grid-column: 1;
        grid-row: 1;
      }
      .discovery-id {
        grid-column: 2;
        grid-row: 1;
      }
      .discovery-family {
        grid-column: 2;
        grid-row: 2;
      }
      .discovery-caps {
        grid-column: 2;
        grid-row: 3;
      }
    }
  `],
})
export class AdminProvidersComponent implements OnInit {
  readonly admin = inject(AdminProvidersService);
  readonly catalog = inject(AdminModelsService);
  private readonly coordinator = inject(AdminModelsCoordinatorService);
  private readonly transloco = inject(TranslocoService);

  readonly switchTab = output<'providers' | 'models'>();

  readonly providers = SYSTEM_PROVIDERS;
  readonly capabilities: CatalogCapability[] = CATALOG_CAPABILITIES;

  // API key form
  readonly keyProvider = signal<SystemProviderValue>('openai');
  readonly keyValue = signal('');
  readonly keyLabel = signal('');
  readonly savingKey = signal(false);
  // Surfaced when a key save fails — otherwise the form silently resets the
  // spinner and looks like nothing happened (mirrors endpointFormError).
  readonly keyFormError = signal<string>('');

  // Post-save discovery dialog. `discoveryProvider` is the provider whose
  // post-save async probe we're polling for; `discoveryPolling` flips off
  // when results arrive (or after we've shown a "took too long" message).
  // `discoverySelections` tracks per-row checkbox + capability override:
  // keys are model_id, values are { selected, capability }. The supported
  // tier is checked-by-default; the generic (default-family) tier is
  // unchecked-by-default — admin opts in deliberately.
  readonly discoveryProvider = signal<SystemProviderValue | null>(null);
  readonly discoveryPolling = signal(false);
  readonly discoveryPayload = signal<DiscoveryResponse | null>(null);
  readonly discoveryShowGeneric = signal(false);
  // Per-row state during the discovery dialog. `selected` controls the
  // checkbox; `capabilities` is the multi-select array. Pre-populated from
  // the orchestrator's `suggested_capabilities` (chunk 3) — admin can
  // tick boxes to refine before insert.
  readonly discoverySelections = signal<
    Record<string, {selected: boolean; capabilities: CatalogCapability[]}>
  >({});
  readonly discoveryAdding = signal(false);
  readonly discoveryError = signal<string>('');

  // Endpoint form
  readonly newEndpointLabel = signal('');
  readonly newEndpointBaseUrl = signal('');
  readonly newEndpointApiKey = signal('');
  readonly newEndpointAllowInsecure = signal(false);
  readonly creatingEndpoint = signal(false);
  readonly endpointFormError = signal<string>('');
  readonly testingEndpointId = signal<string | null>(null);
  readonly testResults = signal<Record<string, LlmEndpointTestResult>>({});

  // Endpoint edit form. Active for at most one row at a time; opens
  // pre-filled with the row's current values. The actual API key is
  // never returned by the backend (only the prefix), so the api_key
  // input represents "replace with this value" — blank means "keep
  // existing", and an explicit checkbox flips to clear_api_key=true.
  readonly editingEndpointId = signal<string | null>(null);
  readonly editEndpointLabel = signal('');
  readonly editEndpointBaseUrl = signal('');
  readonly editEndpointApiKey = signal('');
  readonly editEndpointClearKey = signal(false);
  readonly editEndpointAllowInsecure = signal(false);
  readonly savingEdit = signal(false);
  readonly editFormError = signal<string>('');

  ngOnInit(): void {
    this.admin.loadSystemApiKeys();
    this.admin.loadSystemEndpoints();
  }

  providerLabel(p: string): string {
    return SYSTEM_PROVIDERS.find((x) => x.value === p)?.label ?? p;
  }

  isCodexEndpoint(label: string | null | undefined): boolean {
    return label === 'codex-proxy';
  }

  formatDate(iso: string | null): string {
    if (!iso) return '-';
    return new Date(iso).toLocaleDateString(this.transloco.getActiveLang(), {
      month: 'short',
      day: 'numeric',
    });
  }

  // ── Keys ────────────────────────────────────────────────────────

  onKeyProviderChange(value: string | null): void {
    if (value) this.keyProvider.set(value as SystemProviderValue);
  }

  /**
   * Rows declared with `reconcile: true` in the deployment's Helm values are
   * re-applied on the next `helm upgrade`; make the admin acknowledge that an
   * edit here is temporary before it goes through.
   */
  confirmHelmOverride(row: {managed_by_helm?: boolean} | undefined): boolean {
    if (!row?.managed_by_helm) return true;
    return confirm(this.transloco.translate('admin.helm.confirmOverride'));
  }

  saveKey(): void {
    const value = this.keyValue().trim();
    if (!value) return;
    const provider = this.keyProvider();
    if (!this.confirmHelmOverride(this.admin.systemApiKeys().find((k) => k.provider === provider))) return;
    this.savingKey.set(true);
    this.discoveryError.set('');
    this.keyFormError.set('');
    this.admin
      .setSystemApiKey(provider, {
        api_key: value,
        label: this.keyLabel().trim() || null,
      })
      .subscribe({
        next: () => {
          this.keyValue.set('');
          this.keyLabel.set('');
          this.savingKey.set(false);
          // Trigger the post-save discovery dialog for providers we
          // know how to enumerate. The orchestrator already kicked off
          // an async probe; we poll until the cache is populated.
          if (DISCOVERABLE_PROVIDERS.has(provider)) {
            this.startDiscoveryPolling(provider);
          }
        },
        error: (err) => {
          this.keyFormError.set(err?.error?.detail ?? 'Failed to save key.');
          this.savingKey.set(false);
        },
      });
  }

  /** Poll ``GET .../keys/{provider}/discovery`` until ``ready=true`` (or
   * we've waited long enough). Probes are typically <2s but can stretch to
   * 10s when the provider's listing endpoint is slow; we cap at ~15s and
   * surface a "took too long" message that links to the explicit
   * Rediscover button. */
  private startDiscoveryPolling(provider: SystemProviderValue): void {
    this.discoveryProvider.set(provider);
    this.discoveryPayload.set(null);
    this.discoverySelections.set({});
    this.discoveryShowGeneric.set(false);
    this.discoveryPolling.set(true);

    let attempts = 0;
    const subscription = timer(0, 1500)
      .pipe(
        switchMap(() => {
          attempts += 1;
          return this.admin.getDiscoveryPayload(provider);
        }),
        filter((res: DiscoveryResponse) => res.ready || attempts >= 10),
        take(1),
      )
      .subscribe((res: DiscoveryResponse) => {
        this.discoveryPolling.set(false);
        if (!res.ready) {
          this.discoveryError.set(
            this.transloco.translate('admin.providers.discovery.timeout') ||
              'Discovery is taking longer than expected. Try Rediscover.',
          );
          return;
        }
        this.applyDiscoveryPayload(res);
      });
    void subscription;
  }

  rediscover(): void {
    const provider = this.discoveryProvider();
    if (!provider) return;
    this.discoveryPolling.set(true);
    this.discoveryError.set('');
    this.admin.rediscoverProvider(provider).subscribe({
      next: (res: DiscoveryResponse) => {
        this.discoveryPolling.set(false);
        this.applyDiscoveryPayload(res);
      },
      error: (err) => {
        this.discoveryPolling.set(false);
        this.discoveryError.set(
          err?.error?.detail ?? 'Rediscover failed.',
        );
      },
    });
  }

  private applyDiscoveryPayload(res: DiscoveryResponse): void {
    this.discoveryPayload.set(res);
    const candidates = res.payload?.candidates ?? [];
    const selections: Record<
      string,
      {selected: boolean; capabilities: CatalogCapability[]}
    > = {};
    for (const c of candidates) {
      const caps = c.suggested_capabilities ?? [];
      selections[c.model_id] = {
        selected: c.supported,
        capabilities: [...caps],
      };
    }
    this.discoverySelections.set(selections);
  }

  closeDiscovery(): void {
    this.discoveryProvider.set(null);
    this.discoveryPayload.set(null);
    this.discoverySelections.set({});
    this.discoveryError.set('');
  }

  toggleCandidate(modelId: string, checked: boolean): void {
    const current = this.discoverySelections();
    const entry = current[modelId];
    if (!entry) return;
    this.discoverySelections.set({
      ...current,
      [modelId]: {...entry, selected: checked},
    });
  }

  candidateHasCapability(modelId: string, capability: CatalogCapability): boolean {
    return (
      this.discoverySelections()[modelId]?.capabilities.includes(capability) ??
      false
    );
  }

  toggleCandidateCapability(
    modelId: string,
    capability: CatalogCapability,
    checked: boolean,
  ): void {
    const current = this.discoverySelections();
    const entry = current[modelId];
    if (!entry) return;
    const next = new Set(entry.capabilities);
    if (checked) {
      next.add(capability);
    } else {
      next.delete(capability);
    }
    // Preserve original CATALOG_CAPABILITIES order so the array sent to
    // the backend is deterministic regardless of click order.
    const ordered = this.capabilities.filter((c) => next.has(c));
    this.discoverySelections.set({
      ...current,
      [modelId]: {...entry, capabilities: ordered},
    });
  }

  readonly supportedCandidates = computed(() => {
    const candidates = this.discoveryPayload()?.payload?.candidates ?? [];
    return candidates.filter((c) => c.supported);
  });

  readonly genericCandidates = computed(() => {
    const candidates = this.discoveryPayload()?.payload?.candidates ?? [];
    return candidates.filter((c) => !c.supported);
  });

  readonly selectedCount = computed(() => {
    return Object.values(this.discoverySelections()).filter(
      (s) => s.selected,
    ).length;
  });

  /** Insert one catalog row per checked candidate. We submit sequentially
   * so a single 409/422 doesn't poison the whole batch — the dialog
   * reports successes + failures at the end. */
  addSelectedCandidates(): void {
    const provider = this.discoveryProvider();
    const candidates = this.discoveryPayload()?.payload?.candidates ?? [];
    const selections = this.discoverySelections();
    if (!provider) return;

    const chosen: DiscoveryCandidate[] = candidates.filter(
      (c) => selections[c.model_id]?.selected,
    );
    if (chosen.length === 0) return;

    this.discoveryAdding.set(true);
    this.discoveryError.set('');

    let remaining = chosen.length;
    let failures = 0;
    for (const c of chosen) {
      const selectedCaps = selections[c.model_id]?.capabilities ?? [];
      const capabilities =
        selectedCaps.length > 0 ? selectedCaps : c.suggested_capabilities ?? [];
      this.catalog
        .createModel({
          provider_kind: 'system',
          provider_ref: provider,
          model_id: c.model_id,
          display_label: c.suggested_display_label,
          capabilities,
          family: c.detected_family,
          context_window: null,
        })
        .subscribe({
          next: () => {
            remaining -= 1;
            if (remaining === 0) this.finishAdding(failures);
          },
          error: () => {
            remaining -= 1;
            failures += 1;
            if (remaining === 0) this.finishAdding(failures);
          },
        });
    }
  }

  private finishAdding(failures: number): void {
    this.discoveryAdding.set(false);
    if (failures > 0) {
      this.discoveryError.set(
        `Added with ${failures} failure(s) — likely duplicates that already exist in the catalog.`,
      );
    } else {
      this.closeDiscovery();
    }
  }

  deleteKey(provider: string): void {
    if (!confirm(this.transloco.translate('admin.providers.keys.confirmDelete'))) return;
    if (!this.confirmHelmOverride(this.admin.systemApiKeys().find((k) => k.provider === provider))) return;
    this.admin.deleteSystemApiKey(provider).subscribe();
  }

  // ── Endpoints ───────────────────────────────────────────────────

  createEndpoint(): void {
    const label = this.newEndpointLabel().trim();
    const baseUrl = this.newEndpointBaseUrl().trim();
    if (!label || !baseUrl) return;

    if (!/^https?:\/\//i.test(baseUrl)) {
      this.endpointFormError.set(
        this.transloco.translate('admin.providers.endpoints.errorUrlScheme'),
      );
      return;
    }
    if (baseUrl.toLowerCase().startsWith('http://') && !this.newEndpointAllowInsecure()) {
      this.endpointFormError.set(
        this.transloco.translate('admin.providers.endpoints.errorHttpNeedsOptIn'),
      );
      return;
    }

    this.endpointFormError.set('');
    this.creatingEndpoint.set(true);
    this.admin
      .createSystemEndpoint({
        label,
        base_url: baseUrl,
        api_key: this.newEndpointApiKey().trim() || null,
        allow_insecure: this.newEndpointAllowInsecure(),
      })
      .subscribe({
        next: () => {
          this.newEndpointLabel.set('');
          this.newEndpointBaseUrl.set('');
          this.newEndpointApiKey.set('');
          this.newEndpointAllowInsecure.set(false);
          this.creatingEndpoint.set(false);
        },
        error: (err) => {
          this.endpointFormError.set(err?.error?.detail ?? String(err));
          this.creatingEndpoint.set(false);
        },
      });
  }

  deleteEndpoint(endpointId: string): void {
    if (!confirm(this.transloco.translate('admin.providers.endpoints.confirmDelete'))) return;
    if (!this.confirmHelmOverride(this.admin.systemEndpoints().find((e) => e.id === endpointId))) return;
    this.admin.deleteSystemEndpoint(endpointId).subscribe();
  }

  testEndpoint(endpointId: string): void {
    this.testingEndpointId.set(endpointId);
    this.admin.testSystemEndpoint(endpointId).subscribe({
      next: (result) => {
        this.testResults.update((r) => ({...r, [endpointId]: result}));
        this.testingEndpointId.set(null);
      },
      error: () => this.testingEndpointId.set(null),
    });
  }

  /** Hand off to the Models tab with this endpoint pre-selected and
   * discovery auto-fired. The Models tab subscribes to the coordinator. */
  discoverEndpoint(endpointId: string): void {
    this.coordinator.discoverEndpoint$.next(endpointId);
    this.switchTab.emit('models');
  }

  startEditEndpoint(endpoint: LlmEndpoint): void {
    this.editingEndpointId.set(endpoint.id);
    this.editEndpointLabel.set(endpoint.label);
    this.editEndpointBaseUrl.set(endpoint.base_url);
    this.editEndpointApiKey.set('');
    this.editEndpointClearKey.set(false);
    this.editEndpointAllowInsecure.set(
      endpoint.base_url.toLowerCase().startsWith('http://'),
    );
    this.editFormError.set('');
  }

  cancelEditEndpoint(): void {
    this.editingEndpointId.set(null);
    this.editFormError.set('');
  }

  saveEditEndpoint(): void {
    const endpointId = this.editingEndpointId();
    if (!endpointId) return;
    if (!this.confirmHelmOverride(this.admin.systemEndpoints().find((e) => e.id === endpointId))) return;
    const label = this.editEndpointLabel().trim();
    const baseUrl = this.editEndpointBaseUrl().trim();
    if (!label || !baseUrl) return;

    if (!/^https?:\/\//i.test(baseUrl)) {
      this.editFormError.set(
        this.transloco.translate('admin.providers.endpoints.errorUrlScheme'),
      );
      return;
    }
    if (
      baseUrl.toLowerCase().startsWith('http://') &&
      !this.editEndpointAllowInsecure()
    ) {
      this.editFormError.set(
        this.transloco.translate('admin.providers.endpoints.errorHttpNeedsOptIn'),
      );
      return;
    }

    const newKey = this.editEndpointApiKey().trim();
    const body: LlmEndpointUpdateRequest = {
      label,
      base_url: baseUrl,
      allow_insecure: this.editEndpointAllowInsecure(),
    };
    if (newKey) {
      body.api_key = newKey;
    } else if (this.editEndpointClearKey()) {
      body.clear_api_key = true;
    }

    this.editFormError.set('');
    this.savingEdit.set(true);
    this.admin.updateSystemEndpoint(endpointId, body).subscribe({
      next: () => {
        this.savingEdit.set(false);
        this.editingEndpointId.set(null);
      },
      error: (err) => {
        this.editFormError.set(err?.error?.detail ?? String(err));
        this.savingEdit.set(false);
      },
    });
  }
}
