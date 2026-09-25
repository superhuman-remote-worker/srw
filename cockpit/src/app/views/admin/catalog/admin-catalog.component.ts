import {ChangeDetectionStrategy, Component, computed, DestroyRef, effect, ElementRef, inject, OnInit, signal, viewChild} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {TranslocoService} from '@jsverse/transloco';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {
  CATALOG_CAPABILITIES,
  CatalogCapability,
  CatalogModel,
  CatalogModelTestResult,
  CatalogProviderKind,
  LlmEndpoint,
  LlmEndpointDiscoveredModel,
  LlmEndpointDiscoveryResult,
  SubscriptionDiscoveredModel,
  SubscriptionDiscoveryResult,
  SubscriptionImportResult,
} from '../../../core/models/api.model';
import {TTS_VOICE_CUSTOM, voicesForModelId} from '../../../core/models/tts-voices';
import {AppButtonComponent} from '../../../ui/button';
import {AppInputComponent} from '../../../ui/input';
import {AppSelectComponent} from '../../../ui/select';
import {AppCheckboxComponent} from '../../../ui/checkbox';
import {AppFormFieldComponent} from '../../../ui/form-field';
import {AppBadgeComponent} from '../../../ui/badge';
import {HelmManagedBadgeComponent} from '../../../ui/helm-managed-badge/helm-managed-badge.component';
import {AppDialogComponent} from '../../../ui/dialog';
import {formatTokens, parseTokens} from '../../../core/util/format-tokens';

interface ProviderOption {
  kind: CatalogProviderKind;
  ref: string;
  label: string;
  available: boolean;
}

export type ResearchOperation = 'search' | 'extract' | 'crawl' | 'map';

export const RESEARCH_OPERATIONS: ResearchOperation[] = [
  'search',
  'extract',
  'crawl',
  'map',
];

/** Mirrors src/tools/research/search.ADAPTER_NAMES and each adapter's ops. */
export const SEARCH_ADAPTER_OPTIONS: ReadonlyArray<{
  name: string;
  ops: readonly ResearchOperation[];
}> = [
  {name: 'brave', ops: ['search']},
  {name: 'crawl4ai', ops: ['extract', 'crawl', 'map']},
  {name: 'firecrawl', ops: RESEARCH_OPERATIONS},
  {name: 'searxng', ops: ['search']},
  {name: 'tavily', ops: RESEARCH_OPERATIONS},
];

/**
 * Stable transport marker on `llm_endpoints.transport_kind` for the shared
 * CLIProxyAPI deployment that fronts connected subscription accounts. The
 * frontend branches on this to give that source its subscription affordances
 * (account banner, per-account attribution, bulk import).
 *
 * Never branch on the label: it was renamed `codex-proxy` → `subscription-proxy`
 * and an admin can rename it again. The labels below are only the fallback for
 * a backend that predates the marker.
 */
const SUBSCRIPTION_PROXY_TRANSPORT = 'subscription-proxy';
const LEGACY_SUBSCRIPTION_LABELS = ['codex-proxy', 'subscription-proxy'];

/** True when an endpoint row is the shared subscription proxy. */
function isSubscriptionEndpoint(ep: LlmEndpoint | undefined): boolean {
  if (!ep) return false;
  if (ep.transport_kind === SUBSCRIPTION_PROXY_TRANSPORT) return true;
  return LEGACY_SUBSCRIPTION_LABELS.includes(ep.label);
}

/** The discover route answers one of two shapes; `subscription` picks them apart. */
function isSubscriptionDiscovery(
  result: LlmEndpointDiscoveryResult | SubscriptionDiscoveryResult,
): result is SubscriptionDiscoveryResult {
  return (result as SubscriptionDiscoveryResult).subscription === true;
}

/**
 * Build the `capabilities[]` pre-fill from the discovery hint array.
 * Filters to known capability values and de-duplicates while preserving
 * the order the orchestrator emitted (chat-capable rows already include
 * `auxiliary`; multimodal families include `vision`).
 */
function hintsToCapabilities(
  hints: readonly string[] | null | undefined,
): CatalogCapability[] {
  const known: CatalogCapability[] = [
    'chat',
    'auxiliary',
    'embedding',
    'vision',
    'whisper',
    'tts',
    'search',
    'fetch',
    'rerank',
  ];
  const isKnown = (v: string): v is CatalogCapability =>
    known.includes(v as CatalogCapability);
  const filtered = (hints ?? []).filter(isKnown);
  return filtered.length > 0 ? [...new Set(filtered)] : ['chat', 'auxiliary'];
}

/**
 * Low-window reasoning-starve warning for the Admin → Models form.
 *
 * Reasoning tokens share max_output_tokens, and the agent reserves output off the
 * back of the window: max_output is clamped to the "backstop" =
 * ctx - floor(0.80*ctx) - 4096 (mirrors loader.py _resolve_max_output_tokens /
 * CONTEXT_THRESHOLD_FRACTION + OUTPUT_SAFETY_MARGIN, floored at MIN 4096). At small
 * windows the backstop is the binding cap (the family value only caps at large
 * windows), so a too-low window silently starves reasoning. Warn under ~16k output
 * — the cap that originally truncated minimax mid-reasoning. Returns the warning
 * string, or null when the window is healthy / unset.
 * See knowledge-base/knowledge/features/reasoning_aware_max_output_tokens.md §5.4.
 */
/**
 * The three states `params_json.pricing_id` can express, mirroring
 * `orchestrator/services/openrouter_pricing.py`:
 *
 *  - key absent  → `auto`  : resolve from the model id itself (exact OpenRouter
 *                            id, the once-stripped remainder, then a unique bare
 *                            suffix). Works for `MiniMax-M3`; cannot work for a
 *                            name OpenRouter has never heard of.
 *  - non-empty   → `map`   : resolve against this exact OpenRouter id.
 *  - empty string→ `never` : force unpriced (self-hosted / free). `cost_usd`
 *                            stays NULL, which is NOT the same as $0.00.
 */
export type PricingMode = 'auto' | 'map' | 'never';

export function pricingModeOf(params: Record<string, unknown> | null): PricingMode {
  const raw = params?.['pricing_id'];
  if (typeof raw !== 'string') return 'auto';
  return raw.trim() ? 'map' : 'never';
}

export function pricingIdOf(params: Record<string, unknown> | null): string {
  const raw = params?.['pricing_id'];
  return typeof raw === 'string' ? raw.trim() : '';
}

export function pricingLabelOf(params: Record<string, unknown> | null): string {
  switch (pricingModeOf(params)) {
    case 'map':
      return pricingIdOf(params);
    case 'never':
      return 'Never price';
    default:
      return 'Auto';
  }
}

/**
 * Merge a pricing choice into an existing `params_json`, preserving every other
 * key.
 *
 * PATCH replaces `params_json` **wholesale** (`params_json = $n` in
 * `PostgresDB.update_model` — there is no jsonb merge), so sending
 * `{pricing_id}` on its own would silently delete a TTS `voice` or an inference
 * override that the row already carried. Pass `undefined` to clear the key back
 * to auto-detect, `''` to force-unprice.
 */
export function mergePricingId(
  params: Record<string, unknown> | null,
  pricingId: string | undefined,
): Record<string, unknown> | null {
  const next: Record<string, unknown> = {...(params ?? {})};
  if (pricingId === undefined) {
    delete next['pricing_id'];
  } else {
    next['pricing_id'] = pricingId;
  }
  // A row left with no overrides at all goes back to SQL NULL rather than {}.
  return Object.keys(next).length ? next : null;
}

export function reasoningStarveWarning(ctx: number | null): string | null {
  if (ctx == null || ctx <= 0) return null; // unset → family/default governs
  const backstop = Math.max(4096, ctx - Math.floor(ctx * 0.8) - 4096);
  if (backstop >= 16384) return null; // healthy output room
  return (
    `⚠ Low context window: per-turn output is capped at ~${backstop.toLocaleString()} ` +
    `tokens (≈20% of the window). Reasoning shares this budget, so reasoning models ` +
    `may truncate before answering. Raise the window for more output room.`
  );
}

@Component({
  selector: 'app-admin-catalog',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    HelmManagedBadgeComponent,
    AppButtonComponent,
    AppInputComponent,
    AppSelectComponent,
    AppCheckboxComponent,
    AppFormFieldComponent,
    AppBadgeComponent,
    AppDialogComponent,
  ],
  template: `
    <div class="admin-models">
      <p class="section-intro">
        Curate the LLM offerings available in sessions and jobs.
        Each model anchors to a configured provider (system API key) or a
        system endpoint.
      </p>

      <!-- Models list -->
        <section class="admin-section">
          <h2 class="section-title">Catalog rows</h2>

          @if (models.loading()) {
            <p class="muted">Loading…</p>
          } @else if (models.models().length === 0) {
            <p class="empty-state">
              Add a model from a configured provider to make it available
              in sessions and jobs.
            </p>
          } @else {
            @for (group of groupedModels(); track group.key) {
              <div class="provider-group">
                <h3 class="group-title">{{ group.label }}</h3>
                <div class="model-table">
                  <div class="model-header">
                    <span class="col-display">Display label</span>
                    <span class="col-id">Model ID</span>
                    <span class="col-capability">Capabilities</span>
                    <span class="col-family">Family</span>
                    <span class="col-context">Context</span>
                    <span class="col-pricing">Pricing</span>
                    <span class="col-enabled">Enabled</span>
                    <span class="col-actions"></span>
                  </div>
                  @for (m of group.rows; track m.id) {
                    <div class="model-row">
                      <span class="col-display">
                        {{ m.display_label }}
                        <app-helm-managed-badge [managed]="m.managed_by_helm" [drift]="m.helm_drift" />
                      </span>
                      <span class="col-id mono">{{ m.model_id }}</span>
                      <span class="col-capability">
                        <span class="cap-badges">
                          @for (cap of rowCapabilities(m); track cap) {
                            <app-badge tone="neutral" size="xs">{{ cap }}</app-badge>
                          }
                        </span>
                      </span>
                      <span class="col-family">{{ m.family }}</span>
                      <span
                        class="col-context"
                        [class.muted]="m.context_window_source === 'family_default'"
                        [title]="m.context_window_source === 'family_default' ? 'Family default' : 'Explicit per-model cap'"
                      >{{ fmtTokens(m.resolved_context_window) }}</span>
                      <span class="col-pricing">
                        <button
                          type="button"
                          class="pricing-cell"
                          [class.muted]="pricingModeOf(m.params_json) !== 'map'"
                          [class.mono]="pricingModeOf(m.params_json) === 'map'"
                          [title]="pricingHint(m)"
                          [attr.aria-label]="'Pricing source for ' + m.display_label + ': ' + pricingLabelOf(m.params_json) + '. Edit.'"
                          (click)="openPricing(m)"
                        >{{ pricingLabelOf(m.params_json) }}</button>
                      </span>
                      <span class="col-enabled">
                        <app-checkbox
                          size="sm"
                          [checked]="m.enabled"
                          [ariaLabel]="'Enabled: ' + m.display_label"
                          (changed)="toggleEnabled(m, $event)"
                        />
                      </span>
                      <span class="col-actions">
                        <app-button
                          variant="secondary"
                          size="sm"
                          [loading]="testing() === m.id"
                          [disabled]="testing() === m.id"
                          (clicked)="testRow(m.id)"
                        >
                          {{ testing() === m.id ? 'Testing…' : 'Test' }}
                        </app-button>
                        <app-button
                          variant="danger"
                          size="sm"
                          (clicked)="deleteRow(m)"
                        >
                          Delete
                        </app-button>
                        @if (testResults()[m.id]; as result) {
                          <app-badge
                            [tone]="result.ok ? 'success' : 'danger'"
                            size="xs"
                          >
                            {{ result.ok ? 'OK' : (result.error || 'failed') }}
                          </app-badge>
                        }
                      </span>
                    </div>
                  }
                </div>
              </div>
            }
          }
        </section>

        <!-- Add model form -->
        <section class="admin-section">
          <h2 class="section-title">Add model</h2>

          <div class="create-form">
            <div class="form-row two-col">
              <app-form-field label="Provider">
                <app-select
                  [value]="formProviderKey()"
                  [disabled]="creating()"
                  (changed)="onProviderKeyChange($event)"
                >
                  @for (p of providerOptions(); track p.kind + ':' + p.ref) {
                    <option
                      [value]="p.kind + ':' + p.ref"
                      [disabled]="!p.available"
                    >
                      {{ p.label }}{{ p.available ? '' : ' (not configured)' }}
                    </option>
                  }
                </app-select>
              </app-form-field>
              <app-form-field label="Capabilities">
                <fieldset class="cap-fieldset" [disabled]="creating()">
                  @for (c of capabilities; track c) {
                    <label class="cap-checkbox">
                      <app-checkbox
                        size="sm"
                        [checked]="formCapabilities().includes(c)"
                        [ariaLabel]="c"
                        (changed)="toggleFormCapability(c, $event)"
                      />
                      <span>{{ c }}</span>
                    </label>
                  }
                </fieldset>
              </app-form-field>
            </div>

            @if (selectedEndpointRef(); as endpointRef) {
              <div class="discover-pane" #discoverPane>
                @if (selectedIsSubscription()) {
                  @if (providers.subscriptionAvailability(); as subs) {
                    <div
                      class="subs-banner"
                      [class.ok]="subs.available"
                      [class.warn]="!subs.available"
                    >
                      @if (!subs.reachable) {
                        <span>
                          ⚠ The subscription proxy is not reachable. Enable it in the
                          deployment before registering models here.
                        </span>
                      } @else if (subs.available) {
                        <span>
                          ✓ Subscription proxy active —
                          {{ subs.account_count }} account{{ subs.account_count === 1 ? '' : 's' }}
                          connected
                        </span>
                      } @else {
                        <span>
                          ⚠ No connected subscription. Connect one in
                          Settings → AI Subscriptions before testing models, otherwise
                          dispatched calls will 401.
                        </span>
                      }
                    </div>
                  }
                }
                <div class="discover-actions">
                  <app-button
                    variant="secondary"
                    size="sm"
                    [loading]="discovering()"
                    [disabled]="discovering()"
                    (clicked)="discoverFromEndpoint(endpointRef)"
                  >
                    {{ discovering() ? 'Discovering…' : 'Discover available models' }}
                  </app-button>
                  @if (discoverError()) {
                    <app-badge tone="danger" size="xs">{{ discoverError() }}</app-badge>
                  } @else if (discoveredModels().length > 0) {
                    <span class="muted">
                      Click a model to autofill ID and label:
                    </span>
                  }
                </div>

                <!-- Plain endpoint: unchanged quick-fill chips. -->
                @if (discoveredModels().length > 0) {
                  <div class="discover-list">
                    @for (m of discoveredModels(); track m.id) {
                      <button
                        type="button"
                        class="discover-chip"
                        (click)="applyDiscoveredModel(m)"
                      >
                        <span class="mono">{{ m.id }}</span>
                        <span class="discover-cap">
                          {{ m.capability_hints.join(', ') }}
                        </span>
                      </button>
                    }
                  </div>
                }

                <!-- Subscription proxy: attributed, searchable, bulk-importable. -->
                @if (subscriptionModels().length > 0) {
                  @if (!subscriptionAttributionComplete()) {
                    <p class="subs-incomplete">
                      ⚠ Source attribution is incomplete — one or more accounts could not
                      be read. Models below may show no source; that means unknown, not
                      none.
                    </p>
                  }
                  <div class="subs-toolbar">
                    <app-input
                      [value]="modelFilter()"
                      placeholder="Filter models or sources…"
                      (changed)="modelFilter.set($event)"
                    />
                    <app-button
                      variant="ghost"
                      size="sm"
                      [disabled]="importing()"
                      (clicked)="selectAllSupported()"
                    >
                      Select supported
                    </app-button>
                    <app-button
                      variant="ghost"
                      size="sm"
                      [disabled]="importing() || subscriptionSelection().size === 0"
                      (clicked)="clearSelection()"
                    >
                      Clear
                    </app-button>
                    <app-button
                      variant="primary"
                      size="sm"
                      [loading]="importing()"
                      [disabled]="importing() || subscriptionSelection().size === 0"
                      (clicked)="addSelectedModels()"
                    >
                      Add selected ({{ subscriptionSelection().size }})
                    </app-button>
                    <app-button
                      variant="secondary"
                      size="sm"
                      [loading]="importing()"
                      [disabled]="importing() || importableCount() === 0"
                      (clicked)="addAllSupportedModels()"
                    >
                      Add all supported models ({{ importableCount() }})
                    </app-button>
                  </div>

                  @if (importResult(); as result) {
                    <p class="subs-import-result">
                      Added {{ result.created.length }} · already registered
                      {{ result.skipped.length }} · not importable
                      {{ result.rejected.length }}
                    </p>
                  }

                  <div class="subs-model-list">
                    @for (m of filteredSubscriptionModels(); track m.id) {
                      <div
                        class="subs-model-row"
                        [class.registered]="m.registered"
                        [class.unsupported]="m.support === 'unsupported_modality'"
                      >
                        <app-checkbox
                          size="sm"
                          [checked]="isSelected(m.id)"
                          [disabled]="m.registered || m.support === 'unsupported_modality'"
                          [ariaLabel]="m.id"
                          (changed)="toggleSelection(m, $event)"
                        />
                        <button
                          type="button"
                          class="subs-model-id mono"
                          (click)="applySubscriptionModel(m)"
                          title="Autofill the form with this model"
                        >
                          {{ m.id }}
                        </button>
                        <span class="subs-model-sources">
                          @if (m.sources.length > 0) {
                            @for (source of m.sources; track source) {
                              <app-badge tone="neutral" size="xs">{{ source }}</app-badge>
                            }
                          } @else {
                            <span class="muted">source unknown</span>
                          }
                        </span>
                        @if (m.context_window) {
                          <span class="subs-model-ctx">{{ m.context_window }}</span>
                        }
                        @if (m.registered) {
                          <app-badge tone="success" size="xs">registered</app-badge>
                        }
                        @if (m.routing_drift) {
                          <app-badge tone="warning" size="xs">routing differs</app-badge>
                        }
                        @if (m.support === 'unsupported_modality') {
                          <app-badge tone="danger" size="xs">not a chat model</app-badge>
                        } @else if (m.support === 'needs_review') {
                          <app-badge tone="warning" size="xs">needs review</app-badge>
                        }
                      </div>
                    }
                  </div>
                }
              </div>
            }

            <div class="form-row two-col">
              <app-form-field label="Model ID">
                <app-input
                  [value]="formModelId()"
                  placeholder="e.g. claude-opus-4-7"
                  [disabled]="creating()"
                  (changed)="onModelIdChange($event)"
                />
              </app-form-field>
              <app-form-field label="Display label">
                <app-input
                  [value]="formDisplayLabel()"
                  placeholder="Auto-suggested from ID"
                  [disabled]="creating()"
                  (changed)="formDisplayLabel.set($event)"
                />
              </app-form-field>
            </div>

            @if (researchCapabilitiesSelected()) {
              <div class="form-row two-col research-config">
                <app-form-field label="Search/fetch adapter">
                  <app-select
                    [value]="formSearchProvider()"
                    [disabled]="creating()"
                    (changed)="onSearchProviderChange($event)"
                  >
                    <option value="">Select an adapter</option>
                    @for (adapter of searchAdapterOptions; track adapter.name) {
                      <option [value]="adapter.name">{{ adapter.name }}</option>
                    }
                  </app-select>
                </app-form-field>
                <app-form-field label="Supported operations">
                  <fieldset class="cap-fieldset" [disabled]="creating()">
                    @for (op of researchOperations; track op) {
                      <label class="cap-checkbox">
                        <app-checkbox
                          size="sm"
                          [checked]="formSearchOps().includes(op)"
                          [disabled]="!isResearchOpSupported(op)"
                          [ariaLabel]="op"
                          (changed)="toggleFormSearchOp(op, $event)"
                        />
                        <span>{{ op }}</span>
                      </label>
                    }
                  </fieldset>
                </app-form-field>
              </div>
              @if (researchConfigurationError(); as error) {
                <p class="field-hint field-hint--warn">{{ error }}</p>
              }
            } @else {
              <div class="form-row two-col chat-model-config">
                <app-form-field label="Family">
                  <app-select
                    [value]="formFamily()"
                    [disabled]="creating()"
                    (changed)="onFamilyChange($event)"
                  >
                    @for (f of models.families(); track f) {
                      <option [value]="f">{{ f }}</option>
                    }
                  </app-select>
                </app-form-field>
                <app-form-field label="Context window (optional)">
                  <app-input
                    type="text"
                    list="ctx-window-presets"
                    [value]="formContextWindowText()"
                    [placeholder]="contextWindowPlaceholder()"
                    [disabled]="creating()"
                    (changed)="onContextWindowChange($event)"
                  />
                  <datalist id="ctx-window-presets">
                    @for (p of contextWindowPresets; track p.tokens) {
                      <option [value]="p.label" [label]="p.tokens"></option>
                    }
                  </datalist>
                  @if (contextWindowWarning(); as warn) {
                    <small
                      class="field-hint field-hint--warn"
                      [style.color]="'var(--color-warning, #b45309)'"
                      [style.display]="'block'"
                      [style.margin-top.px]="4"
                    >
                      {{ warn }}
                    </small>
                  }
                </app-form-field>
              </div>
            }

            @if (formCapabilities().includes('tts')) {
              <div class="form-row">
                <app-form-field label="Voice (optional)">
                  @if (ttsVoiceOptions().length > 0) {
                    <app-select
                      [value]="formVoiceCustom() ? CUSTOM_VOICE : formVoice()"
                      [disabled]="creating()"
                      (changed)="onVoiceSelect($event)"
                    >
                      <option value="">(backend default)</option>
                      @for (v of ttsVoiceOptions(); track v) {
                        <option [value]="v">{{ v }}</option>
                      }
                      <option [value]="CUSTOM_VOICE">Custom…</option>
                    </app-select>
                  } @else {
                    <app-input
                      [value]="formVoice()"
                      placeholder="e.g. af_heart, alloy — depends on the TTS backend"
                      [disabled]="creating()"
                      (changed)="formVoice.set($event)"
                    />
                  }
                </app-form-field>
              </div>
              @if (ttsVoiceOptions().length > 0 && formVoiceCustom()) {
                <div class="form-row">
                  <app-form-field label="Custom voice">
                    <app-input
                      [value]="formVoice()"
                      placeholder="exact voice id (must match the backend)"
                      [disabled]="creating()"
                      (changed)="formVoice.set($event)"
                    />
                  </app-form-field>
                </div>
              }
            }

            <div class="form-row">
              <app-form-field label="Pricing ID (optional)">
                <app-input
                  [value]="formPricingId()"
                  [disabled]="creating()"
                  placeholder="OpenRouter ID, e.g. google/gemma-4-26b-a4b-it"
                  (valueChange)="formPricingId.set($event)"
                />
              </app-form-field>
              <p class="field-hint">
                Leave empty to auto-detect from the model ID. Set it when the
                model ID is not an OpenRouter name — otherwise usage meters with
                cost unknown. Editable later from the Pricing column.
              </p>
            </div>

            @if (formError()) {
              <p class="form-error">{{ formError() }}</p>
            }

            <div class="form-row">
              <app-button
                variant="primary"
                size="md"
                [loading]="creating()"
                [disabled]="!canSubmit() || creating()"
                (clicked)="submit()"
              >
                {{ creating() ? 'Adding…' : 'Add model' }}
              </app-button>
            </div>
          </div>
        </section>

        @if (pricingRow(); as row) {
          <app-dialog
            [open]="true"
            title="Pricing source"
            size="md"
            (closed)="closePricing()"
          >
            <p class="dialog-intro">
              How <strong>{{ row.display_label }}</strong> resolves to a $/token
              rate. Rates sync from OpenRouter's public catalog and are snapshotted
              onto each usage event when it is written, so a change here prices
              <em>future</em> usage — it does not reprice history.
            </p>

            <app-form-field label="Source">
              <app-select
                [value]="pricingMode()"
                [disabled]="pricingSaving()"
                (changed)="onPricingModeChange($event)"
              >
                <option value="auto">Auto-detect from the model ID</option>
                <option value="map">Map to an OpenRouter model ID</option>
                <option value="never">Never price (self-hosted / free)</option>
              </app-select>
            </app-form-field>

            @if (pricingMode() === 'map') {
              <app-form-field label="OpenRouter model ID">
                <app-input
                  [value]="pricingDraft()"
                  [disabled]="pricingSaving()"
                  placeholder="e.g. google/gemma-4-26b-a4b-it"
                  (valueChange)="pricingDraft.set($event)"
                />
              </app-form-field>
              <p class="field-hint">
                The full ID as OpenRouter publishes it, provider prefix included.
                Use this for a self-hosted model whose name has no OpenRouter
                equivalent — the rate is then a list-price equivalent, not billed
                spend.
              </p>
            }
            @if (pricingMode() === 'never') {
              <p class="field-hint">
                Usage is still metered in full; only the cost stays unknown
                (<code>null</code>), which the UI must not render as $0.00.
              </p>
            }

            @if (pricingError(); as err) {
              <p class="form-error">{{ err }}</p>
            }

            <div appDialogActions>
              <app-button
                variant="secondary"
                size="sm"
                [disabled]="pricingSaving()"
                (clicked)="closePricing()"
              >Cancel</app-button>
              <app-button
                variant="primary"
                size="sm"
                [loading]="pricingSaving()"
                [disabled]="pricingSaving()"
                (clicked)="savePricing()"
              >Save</app-button>
            </div>
          </app-dialog>
        }
    </div>
  `,
  styles: [`
    :host {
      display: block;
    }
    .admin-models {
      display: block;
    }
    .section-intro {
      font-size: 13px;
      color: var(--text-muted);
      margin: 0 0 16px 0;
    }
    .admin-section { margin-bottom: 32px; }
    .section-title {
      font-size: 16px;
      font-weight: 600;
      margin: 0 0 12px 0;
      color: var(--text-primary);
    }
    .provider-group { margin-bottom: 18px; }
    .group-title {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin: 12px 0 6px 0;
    }
    .model-table {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .model-header,
    .model-row {
      display: grid;
      grid-template-columns: 1.4fr 2fr 160px 100px 100px 150px 70px 260px;
      gap: 8px;
      align-items: center;
      padding: 8px 12px;
      font-size: 13px;
    }
    .model-header {
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 11px;
    }
    .model-row {
      background: var(--surface-0);
      border-radius: var(--radius-control);
      color: var(--text-primary);
    }
    .mono { font-family: ui-monospace, monospace; font-size: 12px; }
    /* Reads as a cell, behaves as a button — the whole value is the hit target
       so there is no separate pencil competing for width in a dense row. */
    .pricing-cell {
      background: none;
      border: none;
      padding: 2px 4px;
      margin: -2px -4px;
      font: inherit;
      color: inherit;
      text-align: left;
      cursor: pointer;
      border-radius: var(--radius-control);
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .pricing-cell:hover,
    .pricing-cell:focus-visible {
      background: var(--hover);
      color: var(--text-primary);
    }
    .dialog-intro {
      margin: 0 0 12px;
      color: var(--text-secondary);
      font-size: 13px;
      line-height: 1.5;
    }
    .field-hint {
      margin: 6px 0 0;
      color: var(--text-muted);
      font-size: 12px;
      line-height: 1.5;
    }
    .empty-state {
      padding: 24px;
      text-align: center;
      color: var(--text-muted);
      background: var(--surface-0);
      border-radius: var(--radius-surface);
    }
    .muted { color: var(--text-muted); }
    .col-actions {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    .cap-badges {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
    }
    .cap-fieldset {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 14px;
      padding: 6px 8px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-control);
      background: var(--surface-0);
    }
    .cap-fieldset[disabled] { opacity: 0.6; pointer-events: none; }
    .cap-checkbox {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      font-size: 12px;
      color: var(--text-primary);
      cursor: pointer;
      user-select: none;
    }
    .create-form {
      padding: 16px;
      background: var(--surface-0);
      border-radius: var(--radius-surface);
    }
    .form-row {
      display: flex;
      gap: 12px;
      margin-bottom: 12px;
    }
    .form-row.two-col > * { flex: 1; }
    .form-error {
      color: var(--danger);
      font-size: 12px;
      margin: 4px 0 8px 0;
    }
    .discover-pane {
      margin-bottom: 12px;
      padding: 10px 12px;
      background: var(--app-bg);
      border: 1px dashed var(--border-color);
      border-radius: var(--radius-surface);
    }
    .discover-actions {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }
    .discover-list {
      margin-top: 10px;
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .discover-chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 4px 10px;
      background: var(--surface-0);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-pill);
      font-size: 12px;
      color: var(--text-primary);
      cursor: pointer;
      font-family: inherit;
    }
    .discover-chip:hover {
      border-color: var(--accent-color);
      color: var(--accent-color);
    }
    .discover-cap {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
    }
    .subs-banner {
      margin-bottom: 10px;
      padding: 6px 10px;
      border-radius: var(--radius-control);
      font-size: 12px;
      line-height: 1.4;
    }
    .subs-banner.ok {
      background: var(--success-tint);
      color: var(--success);
    }
    .subs-banner.warn {
      background: var(--danger-tint);
      color: var(--danger);
    }
    .subs-incomplete {
      margin: 8px 0;
      font-size: 12px;
      color: var(--warning, #b45309);
      line-height: 1.4;
    }
    .subs-toolbar {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      margin: 10px 0;
    }
    .subs-toolbar > app-input {
      flex: 1 1 220px;
    }
    .subs-import-result {
      margin: 0 0 10px 0;
      font-size: 12px;
      color: var(--text-secondary);
    }
    .subs-model-list {
      display: flex;
      flex-direction: column;
      gap: 4px;
      max-height: 320px;
      overflow-y: auto;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      padding: 8px;
    }
    .subs-model-row {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 4px 6px;
      border-radius: var(--radius-control);
      font-size: 12px;
    }
    .subs-model-row.registered {
      opacity: 0.75;
    }
    .subs-model-row.unsupported {
      opacity: 0.6;
    }
    .subs-model-id {
      flex: 1;
      text-align: left;
      background: none;
      border: none;
      padding: 0;
      cursor: pointer;
      color: var(--text-primary);
      font-size: 12px;
    }
    .subs-model-id:hover {
      text-decoration: underline;
    }
    .subs-model-sources {
      display: flex;
      gap: 4px;
      flex-wrap: wrap;
    }
    .subs-model-ctx {
      color: var(--text-muted);
      font-variant-numeric: tabular-nums;
    }
    @media (max-width: 720px) {
      /* The catalog table is a 7-column grid (1.4fr 2fr 160px 100px 100px 70px
         260px = 690px of fixed columns alone), so at phone widths Family /
         Context / Enabled / Test / Delete fall off the right edge and the
         host's overflow:auto quietly hides them. Collapse each row into a
         stacked card so every field and control stays on-screen. Desktop
         (>720px) keeps the table grid untouched. */
      .model-header {
        display: none;
      }
      /* Two-column card: title + enabled-toggle share row 1 (toggle pinned
         top-right, the conventional spot); the model-id becomes a muted
         subtitle so an id identical to the display label reads as a faint echo
         rather than a duplicate; capabilities + family share row 3 (tags left,
         family right); Test/Delete span the bottom. */
      .model-row {
        grid-template-columns: 1fr auto;
        align-items: start;
        column-gap: 8px;
        row-gap: 6px;
        padding: 12px 14px;
      }
      .col-display {
        grid-column: 1;
        grid-row: 1;
        font-size: 15px;
        font-weight: 600;
        min-width: 0;
        overflow-wrap: anywhere;
      }
      .col-enabled {
        grid-column: 2;
        grid-row: 1;
        justify-self: end;
        display: flex;
        align-items: center;
        gap: 6px;
        white-space: nowrap;
      }
      .col-enabled::before {
        content: 'Enabled';
        font-size: 12px;
        color: var(--text-muted);
      }
      .col-id {
        grid-column: 1 / -1;
        grid-row: 2;
        font-size: 11px;
        opacity: 0.6;
      }
      .col-capability {
        grid-column: 1;
        grid-row: 3;
      }
      .col-family {
        grid-column: 2;
        grid-row: 3;
        justify-self: end;
        align-self: start;
        font-size: 12px;
        white-space: nowrap;
      }
      .col-family::before {
        content: 'Family: ';
        color: var(--text-muted);
      }
      .col-context {
        grid-column: 1 / -1;
        grid-row: 4;
        font-size: 12px;
        white-space: nowrap;
      }
      .col-context::before {
        content: 'Context: ';
        color: var(--text-muted);
      }
      .col-pricing {
        grid-column: 1 / -1;
        grid-row: 5;
        font-size: 12px;
        white-space: nowrap;
      }
      .col-pricing::before {
        content: 'Pricing: ';
        color: var(--text-muted);
      }
      .col-actions {
        grid-column: 1 / -1;
        grid-row: 6;
        margin-top: 2px;
      }
      /* Stack the cramped two-up form rows (two ~137px fields side-by-side). */
      .form-row.two-col {
        flex-direction: column;
      }
    }
  `],
})
export class AdminCatalogComponent implements OnInit {
  readonly models = inject(AdminModelsService);
  readonly providers = inject(AdminProvidersService);
  private readonly coordinator = inject(AdminModelsCoordinatorService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly transloco = inject(TranslocoService);

  private readonly discoverPaneRef = viewChild<ElementRef<HTMLElement>>('discoverPane');

  readonly capabilities: CatalogCapability[] = CATALOG_CAPABILITIES;
  /** Shared compact token formatter for the "Context" column. */
  readonly fmtTokens = formatTokens;
  readonly creating = signal(false);
  readonly testing = signal<string | null>(null);
  readonly testResults = signal<Record<string, CatalogModelTestResult>>({});
  readonly formError = signal<string>('');

  // Form state — signals so OnPush picks up updates from primitive callbacks.
  readonly formProviderKey = signal('');
  // Default to ['chat', 'auxiliary'] — chat-capable LLMs always serve
  // auxiliary unless the operator explicitly unchecks it.
  readonly formCapabilities = signal<CatalogCapability[]>(['chat', 'auxiliary']);
  readonly formModelId = signal('');
  readonly formDisplayLabel = signal('');
  readonly formFamily = signal('default');
  readonly formContextWindow = signal<number | null>(null);
  readonly formSearchProvider = signal('');
  readonly formSearchOps = signal<ResearchOperation[]>([]);
  readonly searchAdapterOptions = SEARCH_ADAPTER_OPTIONS;
  readonly researchOperations = RESEARCH_OPERATIONS;
  /**
   * Preset context-window options for the combobox datalist, ascending. Values
   * are ×1024 (binary) — how context windows are actually sized (128k =
   * 131072) — and the hybrid formatTokens() renders them back to these exact
   * labels. Each option shows the label (e.g. "128k") prominently with the raw
   * token count beneath; the field still accepts any custom value typed in.
   */
  readonly contextWindowPresets: {label: string; tokens: number}[] = [
    {label: '64k', tokens: 65_536},
    {label: '128k', tokens: 131_072},
    {label: '256k', tokens: 262_144},
    {label: '384k', tokens: 393_216},
    {label: '512k', tokens: 524_288},
    {label: '1M', tokens: 1_048_576},
  ];
  // Optional TTS voice (e.g. Kokoro af_heart, OpenAI alloy) — persisted into
  // the catalog row's params_json and read by the TTS service. Only sent when
  // the tts capability is selected.
  readonly formVoice = signal('');
  // Optional OpenRouter pricing ID, also params_json. Empty = auto-detect.
  readonly formPricingId = signal('');
  // True when the operator picked "Custom…" (or the backend is unrecognized);
  // then formVoice is a free-text voice id rather than a catalog selection.
  readonly formVoiceCustom = signal(false);
  protected readonly CUSTOM_VOICE = TTS_VOICE_CUSTOM;

  // --- Pricing source ------------------------------------------------------
  // Which OpenRouter catalog entry the rate sync should use for a row. Lives in
  // params_json.pricing_id; before this it was settable only by hand in the DB,
  // which is why self-hosted models sat unpriced and metered as cost NULL.
  /** The row whose pricing dialog is open; null when closed. */
  readonly pricingRow = signal<CatalogModel | null>(null);
  readonly pricingMode = signal<PricingMode>('auto');
  /** Free-text OpenRouter id, only meaningful while pricingMode() === 'map'. */
  readonly pricingDraft = signal('');
  readonly pricingSaving = signal(false);
  readonly pricingError = signal<string | null>(null);
  // Exposed for the template (pure helpers, unit-tested in admin-models.pricing.spec.ts).
  protected readonly pricingModeOf = pricingModeOf;
  protected readonly pricingLabelOf = pricingLabelOf;


  // Mirror for the number input — keeps an empty string when null so the
  // input renders blank instead of "0".
  readonly formContextWindowText = computed(() => {
    const v = this.formContextWindow();
    return v == null ? '' : String(v);
  });

  // Grey hint = the selected family's default window (from the config matrix).
  // Shown while the field is empty; leaving it empty means "inherit this
  // default" (context_window stays null), so we surface it without pinning it
  // as an explicit value.
  readonly contextWindowPlaceholder = computed(() => {
    const def = this.models.familyDefaults()[this.formFamily()];
    return def ? `Family default: ${formatTokens(def)}` : 'e.g. 128k or 131072';
  });

  // Low-window reasoning-starve warning — logic in the pure reasoningStarveWarning()
  // (unit-tested in admin-catalog.warning.spec.ts).
  readonly contextWindowWarning = computed<string | null>(() =>
    reasoningStarveWarning(this.formContextWindow()),
  );

  readonly researchCapabilitiesSelected = computed(() =>
    this.formCapabilities().some((capability) =>
      capability === 'search' || capability === 'fetch',
    ),
  );

  readonly researchConfigurationError = computed<string | null>(() => {
    if (!this.researchCapabilitiesSelected()) return null;
    if (!this.formSearchProvider()) return 'Select a search/fetch adapter.';
    const ops = this.formSearchOps();
    if (ops.length === 0) return 'Select at least one supported operation.';
    const capabilities = this.formCapabilities();
    if (capabilities.includes('search') && !ops.includes('search')) {
      return 'The search capability requires the search operation.';
    }
    if (
      capabilities.includes('fetch') &&
      !ops.some((op) => op === 'extract' || op === 'crawl' || op === 'map')
    ) {
      return 'The fetch capability requires extract, crawl, or map.';
    }
    return null;
  });

  /** Voices for the current model's backend (Kokoro/OpenAI); empty when the
   *  backend isn't recognized → the form falls back to a free-text field. */
  readonly ttsVoiceOptions = computed(() => voicesForModelId(this.formModelId()));

  // Discover-from-endpoint quick-fill state. Resets when the form provider
  // changes (any non-endpoint provider clears the list).
  readonly discovering = signal(false);
  readonly discoveredModels = signal<LlmEndpointDiscoveredModel[]>([]);
  readonly discoverError = signal<string>('');

  // Subscription-proxy discovery: the same probe, enriched server-side with
  // per-account attribution. Kept in its own signals so the plain-endpoint
  // quick-fill path stays exactly as it was.
  readonly subscriptionModels = signal<SubscriptionDiscoveredModel[]>([]);
  readonly subscriptionSelection = signal<ReadonlySet<string>>(new Set<string>());
  readonly subscriptionAttributionComplete = signal(true);
  readonly importing = signal(false);
  readonly importResult = signal<SubscriptionImportResult | null>(null);
  readonly modelFilter = signal('');

  /** Candidates matching the search box, unsupported ones sorted last. */
  readonly filteredSubscriptionModels = computed(() => {
    const needle = this.modelFilter().trim().toLowerCase();
    const rows = this.subscriptionModels();
    if (!needle) return rows;
    return rows.filter(
      (m) =>
        m.id.toLowerCase().includes(needle) ||
        m.display_label.toLowerCase().includes(needle) ||
        m.sources.some((source) => source.toLowerCase().includes(needle)),
    );
  });

  /** Advertised, supported and not yet in the catalog. */
  readonly importableCount = computed(
    () =>
      this.subscriptionModels().filter((m) => m.support === 'supported' && !m.registered)
        .length,
  );

  readonly selectedEndpointRef = computed<string | null>(() => {
    const key = this.formProviderKey();
    if (!key.startsWith('endpoint:')) return null;
    return key.slice('endpoint:'.length);
  });

  /** Provider dropdown options sourced from the keys + endpoints lists.
   * Non-LLM providers (`vision`) are filtered out — they live in
   * `system_api_keys` for env injection but can't anchor a catalog row.
   * The shared subscription proxy is rendered as "Subscription proxy" so
   * admins recognise it as separate from a generic vLLM/Ollama endpoint. */
  readonly providerOptions = computed<ProviderOption[]>(() => {
    const opts: ProviderOption[] = [];
    for (const key of this.providers.systemApiKeys()) {
      if (key.provider === 'vision') continue;
      opts.push({
        kind: 'system',
        ref: key.provider,
        label: key.provider,
        available: true,
      });
    }
    for (const ep of this.providers.systemEndpoints()) {
      opts.push({
        kind: 'endpoint',
        ref: ep.id,
        label: isSubscriptionEndpoint(ep)
          ? 'Subscription proxy'
          : `${ep.label} (endpoint)`,
        // Every system endpoint can anchor a catalog row — including the
        // subscription proxy with nothing signed in (admins may seed rows
        // ahead of a login; the status banner below says when one is needed).
        available: true,
      });
    }
    return opts;
  });

  /** True when the form provider is the shared subscription proxy. */
  readonly selectedIsSubscription = computed(() =>
    isSubscriptionEndpoint(
      this.providers.systemEndpoints().find((e) => e.id === this.selectedEndpointRef()),
    ),
  );

  readonly groupedModels = computed(() => {
    const groups = new Map<string, {key: string; label: string; rows: CatalogModel[]}>();
    for (const m of this.models.models()) {
      const key = `${m.provider_kind}:${m.provider_ref}`;
      const label = m.provider_kind === 'endpoint'
        ? this.endpointLabel(m.provider_ref)
        : m.provider_ref;
      if (!groups.has(key)) {
        groups.set(key, {key, label, rows: []});
      }
      groups.get(key)!.rows.push(m);
    }
    return Array.from(groups.values()).sort((a, b) => a.label.localeCompare(b.label));
  });

  constructor() {
    // Native <select> displays the first option when no value matches, which
    // misleads the operator into thinking a provider is selected when the
    // signal is still empty — leaving the discover pane hidden and the Add
    // button disabled. When exactly one available provider exists, lock the
    // form to it.
    effect(() => {
      const options = this.providerOptions();
      if (this.formProviderKey()) return;
      const available = options.filter((o) => o.available);
      if (available.length !== 1) return;
      const only = available[0];
      this.onProviderKeyChange(`${only.kind}:${only.ref}`);
    });
  }

  ngOnInit(): void {
    this.models.loadModels();
    this.models.loadFamilies();
    this.providers.loadSystemApiKeys();
    this.providers.loadSystemEndpoints();
    this.providers.loadSubscriptionAvailability();

    this.coordinator.discoverEndpoint$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((endpointId) => this.preselectAndDiscover(endpointId));
  }

  /** Cross-tab handoff target — Providers tab fires this when the operator
   * clicks "Discover" on an endpoint card. Resets the form to that
   * endpoint, kicks off the probe, and scrolls the discover pane into
   * view (parent will have already flipped the tab). */
  private preselectAndDiscover(endpointId: string): void {
    this.formProviderKey.set(`endpoint:${endpointId}`);
    this.formModelId.set('');
    this.formDisplayLabel.set('');
    this.formFamily.set('default');
    this.formContextWindow.set(null);
    this.formError.set('');
    this.discoveredModels.set([]);
    this.discoverError.set('');
    this.discoverFromEndpoint(endpointId);
    queueMicrotask(() => {
      this.discoverPaneRef()?.nativeElement.scrollIntoView({
        behavior: 'smooth',
        block: 'center',
      });
    });
  }

  endpointLabel(refId: string): string {
    const ep = this.providers.systemEndpoints().find((e) => e.id === refId);
    if (!ep) return `endpoint:${refId}`;
    return isSubscriptionEndpoint(ep) ? 'Subscription proxy' : `${ep.label} (endpoint)`;
  }

  canSubmit(): boolean {
    return (
      !!this.formProviderKey() &&
      !!this.formModelId().trim() &&
      !!this.formDisplayLabel().trim() &&
      (this.researchCapabilitiesSelected() || !!this.formFamily().trim()) &&
      !this.researchConfigurationError() &&
      this.formCapabilities().length > 0
    );
  }

  /** Capability set displayed for a catalog row in the table. */
  rowCapabilities(m: CatalogModel): CatalogCapability[] {
    return m.capabilities ?? [];
  }

  /** Toggle one capability checkbox in the create/edit form. Maintains
   * CATALOG_CAPABILITIES order so the array sent to the backend is
   * deterministic regardless of click order. */
  toggleFormCapability(capability: CatalogCapability, checked: boolean): void {
    const current = new Set(this.formCapabilities());
    if (checked) {
      current.add(capability);
    } else {
      current.delete(capability);
    }
    const ordered = this.capabilities.filter((c) => current.has(c));
    this.formCapabilities.set(ordered);
  }

  onSearchProviderChange(value: string | null): void {
    const provider = value ?? '';
    this.formSearchProvider.set(provider);
    const supported = new Set(
      SEARCH_ADAPTER_OPTIONS.find((adapter) => adapter.name === provider)?.ops ?? [],
    );
    this.formSearchOps.update((ops) => ops.filter((op) => supported.has(op)));
  }

  isResearchOpSupported(op: ResearchOperation): boolean {
    return !!SEARCH_ADAPTER_OPTIONS
      .find((adapter) => adapter.name === this.formSearchProvider())
      ?.ops.includes(op);
  }

  toggleFormSearchOp(op: ResearchOperation, checked: boolean): void {
    if (checked && !this.isResearchOpSupported(op)) return;
    const selected = new Set(this.formSearchOps());
    if (checked) selected.add(op);
    else selected.delete(op);
    this.formSearchOps.set(RESEARCH_OPERATIONS.filter((candidate) => selected.has(candidate)));
  }

  onProviderKeyChange(value: string | null): void {
    this.formProviderKey.set(value ?? '');
    this.discoveredModels.set([]);
    this.discoverError.set('');
    // Provider change invalidates any prior model-specific autofill — wiping
    // the form fields prevents a stale model_id from being submitted under a
    // different provider.
    this.formModelId.set('');
    this.formDisplayLabel.set('');
    this.formFamily.set('default');
    this.formContextWindow.set(null);
    this.formError.set('');
  }

  onFamilyChange(value: string | null): void {
    if (value !== null) this.formFamily.set(value);
  }

  onContextWindowChange(text: string): void {
    // Accepts preset labels ("128k"/"1M", ×1024) and raw custom numbers.
    this.formContextWindow.set(parseTokens(text));
  }

  /** Voice dropdown change: a real voice sets formVoice directly; the
   *  "Custom…" sentinel switches to the free-text field. */
  onVoiceSelect(value: string | null): void {
    if (value === this.CUSTOM_VOICE) {
      this.formVoiceCustom.set(true);
      this.formVoice.set('');
    } else {
      this.formVoiceCustom.set(false);
      this.formVoice.set(value ?? '');
    }
  }

  submit(): void {
    this.formError.set('');
    const [kind, ref] = this.formProviderKey().split(':') as [CatalogProviderKind, string];
    if (!kind || !ref) {
      this.formError.set('Pick a provider.');
      return;
    }
    const capabilities = this.formCapabilities();
    if (capabilities.length === 0) {
      this.formError.set('Select at least one capability.');
      return;
    }
    const researchError = this.researchConfigurationError();
    if (researchError) {
      this.formError.set(researchError);
      return;
    }
    const voice = this.formVoice().trim();
    const pricingId = this.formPricingId().trim();
    // Both ride in the catalog row's params_json: voice is TTS-only, pricing_id
    // points the OpenRouter rate sync at a specific catalog entry.
    const params: Record<string, unknown> = {};
    if (capabilities.includes('tts') && voice) params['voice'] = voice;
    if (pricingId) params['pricing_id'] = pricingId;
    if (this.researchCapabilitiesSelected()) {
      params['provider'] = this.formSearchProvider();
      params['ops'] = this.formSearchOps();
    }
    const paramsJson = Object.keys(params).length ? params : undefined;
    this.creating.set(true);
    this.models
      .createModel({
        provider_kind: kind,
        provider_ref: ref,
        model_id: this.formModelId().trim(),
        display_label: this.formDisplayLabel().trim(),
        capabilities,
        family: this.researchCapabilitiesSelected() ? 'default' : this.formFamily().trim(),
        context_window: this.researchCapabilitiesSelected()
          ? null
          : this.formContextWindow() ?? null,
        params_json: paramsJson,
      })
      .subscribe({
        next: () => {
          this.formModelId.set('');
          this.formDisplayLabel.set('');
          this.formContextWindow.set(null);
          this.formVoice.set('');
          this.formVoiceCustom.set(false);
          this.formPricingId.set('');
          this.formSearchProvider.set('');
          this.formSearchOps.set([]);
          this.creating.set(false);
        },
        error: (err) => {
          this.formError.set(err?.error?.detail ?? 'Failed to add model.');
          this.creating.set(false);
        },
      });
  }

  /**
   * Rows declared with `reconcile: true` in the deployment's Helm values are
   * re-applied on the next `helm upgrade`; an edit here is temporary and the
   * admin should say so knowingly.
   */
  confirmHelmOverride(model: CatalogModel | null | undefined): boolean {
    if (!model?.managed_by_helm) return true;
    return confirm(this.transloco.translate('admin.helm.confirmOverride'));
  }

  toggleEnabled(model: CatalogModel, checked: boolean): void {
    if (!this.confirmHelmOverride(model)) return;
    this.models.updateModel(model.id, {enabled: checked}).subscribe();
  }

  /** Hover/title text spelling out what the cell's shorthand actually means. */
  pricingHint(m: CatalogModel): string {
    switch (pricingModeOf(m.params_json)) {
      case 'map':
        return `Priced from OpenRouter model ${pricingIdOf(m.params_json)}.`;
      case 'never':
        return 'Explicitly never priced — usage is metered, cost stays unknown.';
      default:
        return (
          `Resolved from the model ID (${m.model_id}). Unpriced if OpenRouter ` +
          'publishes no model under that name.'
        );
    }
  }

  openPricing(m: CatalogModel): void {
    this.pricingRow.set(m);
    this.pricingMode.set(pricingModeOf(m.params_json));
    this.pricingDraft.set(pricingIdOf(m.params_json));
    this.pricingError.set(null);
  }

  closePricing(): void {
    if (this.pricingSaving()) return;
    this.pricingRow.set(null);
  }

  onPricingModeChange(mode: string | null): void {
    this.pricingMode.set((mode ?? 'auto') as PricingMode);
    this.pricingError.set(null);
  }

  savePricing(): void {
    const row = this.pricingRow();
    if (!row) return;
    if (!this.confirmHelmOverride(row)) return;
    const mode = this.pricingMode();
    const id = this.pricingDraft().trim();
    if (mode === 'map' && !id) {
      this.pricingError.set('Enter an OpenRouter model ID, or choose auto-detect.');
      return;
    }
    // undefined removes the key (auto), '' forces unpriced, else the mapping.
    const pricingId = mode === 'auto' ? undefined : mode === 'never' ? '' : id;
    this.pricingSaving.set(true);
    this.models
      .updateModel(row.id, {params_json: mergePricingId(row.params_json, pricingId)})
      .subscribe({
        next: () => {
          this.pricingSaving.set(false);
          this.pricingRow.set(null);
        },
        error: (err) => {
          this.pricingError.set(err?.error?.detail ?? 'Failed to save pricing source.');
          this.pricingSaving.set(false);
        },
      });
  }

  deleteRow(model: CatalogModel): void {
    if (!confirm(`Delete "${model.display_label}" from the catalog?`)) return;
    if (!this.confirmHelmOverride(model)) return;
    this.models.deleteModel(model.id).subscribe((res) => {
      if (res.warning) alert(res.warning);
    });
  }

  testRow(id: string): void {
    this.testing.set(id);
    this.models.testModel(id).subscribe({
      next: (result) => {
        this.testResults.update((curr) => ({...curr, [id]: result}));
        this.testing.set(null);
      },
      error: () => this.testing.set(null),
    });
  }

  discoverFromEndpoint(endpointId: string): void {
    this.discovering.set(true);
    this.discoverError.set('');
    this.discoveredModels.set([]);
    this.subscriptionModels.set([]);
    this.subscriptionSelection.set(new Set<string>());
    this.importResult.set(null);
    this.providers.discoverSystemEndpointModels(endpointId).subscribe({
      next: (result) => {
        this.discovering.set(false);
        if (!result.ok) {
          // A failed discovery is not an authoritative empty inventory —
          // surface the error and leave the previous catalog untouched.
          this.discoverError.set(result.error || 'Discovery failed.');
          return;
        }
        if (isSubscriptionDiscovery(result)) {
          this.subscriptionModels.set(result.models);
          this.subscriptionAttributionComplete.set(result.attribution_complete);
          return;
        }
        this.discoveredModels.set(result.models);
      },
      error: (err) => {
        this.discovering.set(false);
        this.discoverError.set(err?.error?.detail ?? 'Discovery failed.');
      },
    });
  }

  // ── Subscription bulk import ──────────────────────────────────

  isSelected(modelId: string): boolean {
    return this.subscriptionSelection().has(modelId);
  }

  toggleSelection(model: SubscriptionDiscoveredModel, checked: boolean): void {
    // Registered rows and unsupported modalities are never selectable — the
    // backend refuses them anyway, and offering the checkbox would imply the
    // import could overwrite an admin's edits.
    if (model.registered || model.support === 'unsupported_modality') return;
    const next = new Set(this.subscriptionSelection());
    if (checked) next.add(model.id);
    else next.delete(model.id);
    this.subscriptionSelection.set(next);
  }

  selectAllSupported(): void {
    this.subscriptionSelection.set(
      new Set(
        this.filteredSubscriptionModels()
          .filter((m) => m.support === 'supported' && !m.registered)
          .map((m) => m.id),
      ),
    );
  }

  clearSelection(): void {
    this.subscriptionSelection.set(new Set<string>());
  }

  /** "Add selected" — registers exactly the checked candidates. */
  addSelectedModels(): void {
    const ids = Array.from(this.subscriptionSelection());
    if (ids.length === 0) return;
    const includeReview = this.subscriptionModels().some(
      (m) => ids.includes(m.id) && m.support === 'needs_review',
    );
    this.runImport(ids, includeReview);
  }

  /** "Add all supported models" — server-side selection, never the filter. */
  addAllSupportedModels(): void {
    this.runImport(undefined, false);
  }

  private runImport(modelIds: string[] | undefined, includeReview: boolean): void {
    const endpointId = this.selectedEndpointRef();
    if (!endpointId) return;
    this.importing.set(true);
    this.importResult.set(null);
    this.providers.importSubscriptionModels(endpointId, modelIds, includeReview).subscribe({
      next: (result) => {
        this.importing.set(false);
        this.importResult.set(result);
        this.clearSelection();
        // Re-run discovery so the registration state (and any drift) is read
        // back from the catalog rather than assumed from the import result.
        this.discoverFromEndpoint(endpointId);
      },
      error: (err) => {
        this.importing.set(false);
        this.discoverError.set(err?.error?.detail ?? 'Import failed.');
      },
    });
  }

  /** Pre-fill the single-model form from a subscription candidate. */
  applySubscriptionModel(m: SubscriptionDiscoveredModel): void {
    this.formModelId.set(m.id);
    this.formDisplayLabel.set(m.display_label || m.id);
    this.formCapabilities.set(hintsToCapabilities(m.capability_hints));
    if (m.family) {
      this.formFamily.set(m.family);
    } else {
      this.detectAndSetFamily(m.id);
    }
    this.formContextWindow.set(m.context_window ?? null);
  }

  applyDiscoveredModel(m: LlmEndpointDiscoveredModel): void {
    this.formModelId.set(m.id);
    this.formDisplayLabel.set(m.id);
    this.formCapabilities.set(hintsToCapabilities(m.capability_hints));
    // Endpoint discovery doesn't always know the family — fall back to the
    // matcher when the discovered row leaves it empty.
    if (m.family) {
      this.formFamily.set(m.family);
    } else {
      this.detectAndSetFamily(m.id);
    }
    this.formContextWindow.set(m.context_window ?? null);
  }

  /** Invoked when the operator types or pastes a model ID directly (not
   * via the discovery list). Pre-fills the family dropdown from the
   * matcher; admin can still override before save. */
  onModelIdChange(value: string): void {
    this.formModelId.set(value);
    const trimmed = value.trim();
    if (!trimmed) {
      this.formFamily.set('default');
      return;
    }
    this.detectAndSetFamily(trimmed);
  }

  private detectAndSetFamily(modelId: string): void {
    this.models.detectFamily(modelId).subscribe((res) => {
      // Only apply the matcher's suggestion when it actually matched; a
      // 'fallback' result means we'd just be replacing whatever the admin
      // already selected with 'default', which is rarely what they want.
      if (res.source === 'matched') {
        this.formFamily.set(res.family);
      }
    });
  }
}
