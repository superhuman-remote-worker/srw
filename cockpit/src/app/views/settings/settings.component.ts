import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  OnDestroy,
  OnInit,
  signal,
} from '@angular/core';
import { Router } from '@angular/router';
import { environment } from '../../core/environment';
import { McpTokenService } from '../../core/services/mcp-token.service';
import { UserService } from '../../core/services/user.service';
import { ViewModeService } from '../../core/services/view-mode.service';
import { ApiService } from '../../core/services/api.service';
import {
  SettingsService,
  MainCloudSettingsResponse,
  MainCloudFormState,
} from '../../core/services/settings.service';
import { ModelService } from '../../core/services/model.service';
import { CapabilitiesService } from '../../core/services/capabilities.service';
import { I18nService, SupportedLang } from '../../core/services/i18n.service';
import type { ExpertEditorNavigationState } from '../experts/expert-editor.component';
import {
  ApiKeyProvider,
  SubscriptionLogin,
  SubscriptionsStatus,
  SubscriptionUsage,
  CommunicationSettings,
  Expert,
  ExpertDefaultsResponse,
  McpTokenCreateResponse,
  Project,
  ReadAloudReasoningLevel,
} from '../../core/models/api.model';
import {
  voicesForModelId,
  voiceLanguageTag,
  ttsBackendForModelId,
  TtsAccountVoice,
  TtsLibraryVoice,
} from '../../core/models/tts-voices';
import { SidebarToggleComponent } from '../../shell/sidebar-toggle/sidebar-toggle.component';
import { AppThemeToggleComponent } from '../../ui/theme-toggle';
import { AppAccentToggleComponent } from '../../ui/accent-toggle';
import { TranslocoPipe, TranslocoService } from '@jsverse/transloco';
import { AppButtonComponent } from '../../ui/button';
import { AppInputComponent } from '../../ui/input';
import { AppTextareaComponent } from '../../ui/textarea';
import { AppSelectComponent } from '../../ui/select';
import { AppSwitchComponent } from '../../ui/switch';
import { AppCheckboxComponent } from '../../ui/checkbox';
import { AppFormFieldComponent } from '../../ui/form-field';
import { AppBadgeComponent } from '../../ui/badge';
import { AppIconComponent } from '../../ui/icon';

const PROVIDERS: { value: ApiKeyProvider; label: string }[] = [
  { value: 'openai', label: 'OpenAI' },
  { value: 'anthropic', label: 'Anthropic' },
  { value: 'google', label: 'Google' },
  { value: 'groq', label: 'Groq' },
  { value: 'openrouter', label: 'OpenRouter' },
  { value: 'mistral', label: 'Mistral' },
  { value: 'codex', label: 'Codex' },
  { value: 'vision', label: 'Vision' },
];

const EXPIRY_OPTIONS = [
  { value: '', label: '' },
  { value: '30', label: '' },
  { value: '90', label: '' },
  { value: '365', label: '' },
];

@Component({
  selector: 'app-settings',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    SidebarToggleComponent,
    AppThemeToggleComponent,
    AppAccentToggleComponent,
    TranslocoPipe,
    AppButtonComponent,
    AppInputComponent,
    AppTextareaComponent,
    AppSelectComponent,
    AppSwitchComponent,
    AppCheckboxComponent,
    AppFormFieldComponent,
    AppBadgeComponent,
    AppIconComponent,
  ],
  template: `
    <div class="settings-page">
      <div class="settings-container">
        <div class="page-header">
          <app-sidebar-toggle />
          <h1 class="page-title">{{ 'settings.title' | transloco }}</h1>
        </div>

        <!-- Appearance Section -->
        <section class="settings-section">
          <h2 class="section-title">{{ 'settings.appearance.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.appearance.desc' | transloco }}</p>
          <div class="form-block">
            <app-form-field [label]="'settings.appearance.themeLabel' | transloco">
              <app-theme-toggle
                [showLabels]="true"
                [ariaLabel]="'settings.appearance.themeLabel' | transloco"
              />
            </app-form-field>
            <app-form-field [label]="'settings.appearance.accentLabel' | transloco">
              <app-accent-toggle [ariaLabel]="'settings.appearance.accentLabel' | transloco" />
            </app-form-field>
          </div>
        </section>

        <!-- Language Section -->
        <section class="settings-section">
          <h2 class="section-title">{{ 'settings.language.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.language.desc' | transloco }}</p>
          <div class="form-block">
            <app-form-field [label]="'settings.language.label' | transloco">
              <app-select [value]="i18n.activeLang()" (changed)="onLanguageChange($any($event))">
                <option value="en">English</option>
                <option value="de-DE">Deutsch</option>
              </app-select>
            </app-form-field>
          </div>
        </section>

        <!-- Read-aloud voice -->
        @if (ttsConfigured()) {
          <section class="settings-section">
            <h2 class="section-title">{{ 'settings.voice.title' | transloco }}</h2>
            <p class="section-desc">{{ 'settings.voice.desc' | transloco }}</p>
            <div class="form-block">
              <app-form-field
                [label]="'settings.voice.provider' | transloco"
                [hint]="'settings.voice.providerHint' | transloco"
              >
                <app-select [value]="ttsModel()" (changed)="setTtsModel($any($event))">
                  @for (m of modelService.ttsModels(); track m.id) {
                    <option [value]="m.id">
                      {{ m.label
                      }}{{
                        !ttsModelOverridden() && m.id === resolved().default_tts_model
                          ? ' (' + ('common.default' | transloco) + ')'
                          : ''
                      }}
                    </option>
                  }
                </app-select>
              </app-form-field>
              <app-form-field [label]="'settings.voice.label' | transloco">
                @if (ttsBackend() === 'elevenlabs') {
                  @if (elevenVoicesLoading()) {
                    <p class="voice-lang-note">{{ 'settings.voice.loadingVoices' | transloco }}</p>
                  } @else if (elevenVoices().length > 0) {
                    <app-select [value]="ttsVoice()" (changed)="setTtsVoice($any($event))">
                      <option value="">{{ 'settings.voice.auto' | transloco }}</option>
                      @for (v of elevenVoices(); track v.id) {
                        <option [value]="v.id">{{ elevenVoiceLabel(v) }}</option>
                      }
                    </app-select>
                  } @else {
                    <app-input
                      [value]="ttsVoice()"
                      [placeholder]="'settings.voice.customPlaceholder' | transloco"
                      (changed)="setTtsVoice($event)"
                    />
                  }
                } @else if (ttsVoices().length > 0) {
                  <app-select [value]="ttsVoice()" (changed)="setTtsVoice($any($event))">
                    <option value="">{{ 'settings.voice.auto' | transloco }}</option>
                    @for (v of ttsVoices(); track v) {
                      <option [value]="v">{{ voiceOptionLabel(v) }}</option>
                    }
                  </app-select>
                } @else {
                  <app-input
                    [value]="ttsVoice()"
                    [placeholder]="'settings.voice.customPlaceholder' | transloco"
                    (changed)="setTtsVoice($event)"
                  />
                }
              </app-form-field>
              @if (ttsVoices().length > 0) {
                <p class="voice-lang-note">
                  {{ 'settings.voice.langNote' | transloco }}
                  @if (ttsBackend() === 'kokoro') {
                    {{ 'settings.voice.kokoroNoGerman' | transloco }}
                  }
                </p>
              }
              <app-form-field [label]="'settings.voice.sampleLabel' | transloco">
                <app-textarea
                  [value]="previewText()"
                  (valueChange)="onPreviewTextChange($event)"
                  [rows]="2"
                  size="sm"
                  [placeholder]="'settings.voice.samplePlaceholder' | transloco"
                />
                <p class="voice-sample-hint">
                  {{ 'settings.voice.sampleHint' | transloco: { left: previewCharsLeft() } }}
                </p>
              </app-form-field>
              <div class="voice-preview-row">
                <app-button
                  variant="secondary"
                  size="sm"
                  [loading]="previewingVoice()"
                  [disabled]="previewingVoice()"
                  (clicked)="previewVoice()"
                >
                  <app-icon size="sm">play_arrow</app-icon>
                  {{ 'settings.voice.preview' | transloco }}
                </app-button>
                @if (ttsBackend() === 'elevenlabs' && selectedElevenVoice()?.preview_url) {
                  <app-button variant="ghost" size="sm" (clicked)="playHostedPreview()">
                    <app-icon size="sm">graphic_eq</app-icon>
                    {{ 'settings.voice.hostedPreview' | transloco }}
                  </app-button>
                }
                @if (previewErrorKey()) {
                  <span class="voice-preview-error">{{ previewErrorKey()! | transloco }}</span>
                }
              </div>
              @if (ttsBackend() === 'elevenlabs' && selectedElevenVoice()?.preview_url) {
                <p class="voice-lang-note">{{ 'settings.voice.previewCaveat' | transloco }}</p>
              }

              <!-- How the message is rewritten for speech (all backends). The
                   auxiliary LLM cleans markdown + shapes the text; these two
                   knobs steer it — reasoning (off by default; on = smarter but
                   slower first audio) and a free-text instruction the user
                   controls (skip tables, TLDR long messages, omit file names…). -->
              <div class="voice-rewrite">
                <h3 class="voice-subhead">{{ 'settings.voice.rewriteTitle' | transloco }}</h3>
                <app-form-field
                  [label]="'settings.voice.rewriteReasoningLabel' | transloco"
                  [hint]="'settings.voice.rewriteReasoningHint' | transloco"
                >
                  <app-select
                    [value]="readAloudReasoning()"
                    (changed)="readAloudReasoning.set($any($event))"
                  >
                    <option value="off">{{ 'settings.voice.reasoningOff' | transloco }}</option>
                    <option value="low">{{ 'settings.voice.reasoningLow' | transloco }}</option>
                    <option value="medium">
                      {{ 'settings.voice.reasoningMedium' | transloco }}
                    </option>
                    <option value="high">{{ 'settings.voice.reasoningHigh' | transloco }}</option>
                  </app-select>
                </app-form-field>
                <app-form-field
                  [label]="'settings.voice.rewritePromptLabel' | transloco"
                  [hint]="'settings.voice.rewritePromptHint' | transloco"
                >
                  <app-textarea
                    [value]="readAloudPromptDraft()"
                    (valueChange)="onReadAloudPromptChange($event)"
                    [rows]="3"
                    size="sm"
                    [placeholder]="'settings.voice.rewritePromptPlaceholder' | transloco"
                  />
                  <p class="voice-sample-hint">
                    {{
                      'settings.voice.rewritePromptCounter'
                        | transloco: { left: readAloudPromptCharsLeft() }
                    }}
                  </p>
                </app-form-field>
                <div class="actions-row">
                  <app-button
                    variant="primary"
                    size="sm"
                    [loading]="savingReadAloud()"
                    [disabled]="savingReadAloud()"
                    (clicked)="saveReadAloud()"
                  >
                    {{
                      savingReadAloud()
                        ? ('common.saving' | transloco)
                        : ('settings.voice.rewriteSave' | transloco)
                    }}
                  </app-button>
                  @if (readAloudSaved()) {
                    <app-badge tone="success" size="sm">{{ 'common.saved' | transloco }}</app-badge>
                  }
                </div>
              </div>

              <!-- ElevenLabs Voice Library browser (Phase 6): search the 10k+
                   community library, audition via hosted previews, and (when the
                   admin add-gate is on) copy a voice into the deployment account. -->
              @if (ttsBackend() === 'elevenlabs') {
                <div class="voice-library">
                  <div class="voice-library-head">
                    <app-button variant="ghost" size="sm" (clicked)="toggleLibrary()">
                      <app-icon size="sm">{{
                        libraryOpen() ? 'expand_less' : 'travel_explore'
                      }}</app-icon>
                      {{ 'settings.voice.libraryToggle' | transloco }}
                    </app-button>
                    @if (userService.currentUser()?.is_admin) {
                      <label class="voice-library-flag">
                        <app-switch
                          size="sm"
                          [checked]="ttsLibraryFlag()"
                          [disabled]="ttsLibraryFlagSaving()"
                          (changed)="setTtsLibraryFlag($event)"
                        />
                        <span>{{ 'settings.voice.libraryAdminFlag' | transloco }}</span>
                      </label>
                    }
                  </div>

                  @if (libraryOpen()) {
                    <div class="voice-library-search">
                      <app-input
                        [value]="librarySearch()"
                        [placeholder]="'settings.voice.librarySearchPlaceholder' | transloco"
                        (changed)="librarySearch.set($event)"
                      />
                      <app-select
                        [value]="libraryGender()"
                        (changed)="libraryGender.set($any($event))"
                      >
                        <option value="">
                          {{ 'settings.voice.libraryAnyGender' | transloco }}
                        </option>
                        <option value="female">{{ 'settings.voice.female' | transloco }}</option>
                        <option value="male">{{ 'settings.voice.male' | transloco }}</option>
                      </app-select>
                      <app-button
                        variant="secondary"
                        size="sm"
                        [loading]="libraryLoading()"
                        [disabled]="libraryLoading()"
                        (clicked)="searchLibrary()"
                      >
                        <app-icon size="sm">search</app-icon>
                        {{ 'settings.voice.librarySearch' | transloco }}
                      </app-button>
                    </div>

                    @if (libraryError()) {
                      <p class="voice-preview-error">{{ libraryError() }}</p>
                    }
                    @if (libraryVoices().length > 0) {
                      <p class="voice-lang-note">
                        {{ 'settings.voice.libraryPreviewCaveat' | transloco }}
                      </p>
                    }

                    <div class="voice-library-results">
                      @for (v of libraryVoices(); track v.id) {
                        <div class="voice-library-card" [class.is-added]="libraryAdded() === v.id">
                          <div class="voice-library-card__info">
                            <span class="voice-library-card__name">{{ v.name }}</span>
                            @if (libraryVoiceLabel(v)) {
                              <span class="voice-library-card__tags">{{
                                libraryVoiceLabel(v)
                              }}</span>
                            }
                          </div>
                          <div class="voice-library-card__actions">
                            @if (v.preview_url) {
                              <app-button
                                variant="ghost"
                                size="sm"
                                [ariaLabel]="'settings.voice.hostedPreview' | transloco"
                                (clicked)="playLibrarySample(v)"
                              >
                                <app-icon size="sm">graphic_eq</app-icon>
                              </app-button>
                            }
                            @if (libraryAddEnabled()) {
                              @if (libraryAdded() === v.id) {
                                <span class="voice-library-card__done">
                                  <app-icon size="sm">check</app-icon>
                                  {{ 'settings.voice.libraryAdded' | transloco }}
                                </span>
                              } @else {
                                <app-button
                                  variant="secondary"
                                  size="sm"
                                  [loading]="libraryAddingId() === v.id"
                                  [disabled]="libraryAddingId() !== null"
                                  (clicked)="addLibraryVoice(v)"
                                >
                                  {{ 'settings.voice.libraryAdd' | transloco }}
                                </app-button>
                              }
                            }
                          </div>
                        </div>
                      }
                      @if (!libraryLoading() && libraryVoices().length === 0 && !libraryError()) {
                        <p class="voice-lang-note">
                          {{ 'settings.voice.libraryEmpty' | transloco }}
                        </p>
                      }
                    </div>
                  }
                </div>
              }
            </div>
          </section>
        }

        <!-- Data Visibility Section (Admin Only) -->
        @if (userService.currentUser()?.is_admin) {
          <section class="settings-section">
            <h2 class="section-title">{{ 'settings.dataVisibility.title' | transloco }}</h2>
            <p class="section-desc">{{ 'settings.dataVisibility.desc' | transloco }}</p>
            <div class="form-block">
              <app-form-field [label]="'settings.dataVisibility.label' | transloco">
                <app-select
                  [value]="viewMode.viewMode()"
                  (changed)="viewMode.setMode($any($event))"
                >
                  <option value="all">{{ 'settings.dataVisibility.optionAll' | transloco }}</option>
                  <option value="me">{{ 'settings.dataVisibility.optionMe' | transloco }}</option>
                </app-select>
              </app-form-field>
            </div>
          </section>
        }

        <!-- LLM provider keys: the user's own, used ahead of the system keys.
             Not the PAT page (settings.apiKeys.*, linked further down). -->
        <section class="settings-section">
          <h2 class="section-title">{{ 'settings.providerKeys.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.providerKeys.desc' | transloco }}</p>

          <!-- Key List -->
          @if (settingsService.apiKeys().length > 0) {
            <div class="key-table">
              <div class="key-header">
                <span class="col-provider">{{ 'settings.providerKeys.colProvider' | transloco }}</span>
                <span class="col-prefix">{{ 'settings.providerKeys.colKey' | transloco }}</span>
                <span class="col-label">{{ 'settings.providerKeys.colLabel' | transloco }}</span>
                <span class="col-updated">{{ 'settings.providerKeys.colUpdated' | transloco }}</span>
                <span class="col-action"></span>
              </div>
              @for (key of settingsService.apiKeys(); track key.id) {
                <div class="key-row">
                  <span class="col-provider">{{ providerLabel(key.provider) }}</span>
                  <span class="col-prefix mono">{{ key.key_prefix }}...</span>
                  <span class="col-label">{{ key.label || '-' }}</span>
                  <span class="col-updated">{{ formatDate(key.updated_at) }}</span>
                  <span class="col-action">
                    <app-button variant="danger" size="sm" (clicked)="deleteApiKey(key.provider)">
                      {{ 'common.delete' | transloco }}
                    </app-button>
                  </span>
                </div>
              }
            </div>
          } @else {
            <p class="empty-state">{{ 'settings.providerKeys.empty' | transloco }}</p>
          }

          <!-- Set Key Form -->
          <div class="create-form">
            <h3 class="form-title">{{ 'settings.providerKeys.addTitle' | transloco }}</h3>
            <div class="form-row two-col">
              <app-select
                [value]="keyProvider()"
                [disabled]="settingKey()"
                (changed)="onKeyProviderChange($event)"
              >
                @for (p of providers; track p.value) {
                  <option [value]="p.value">{{ p.label }}</option>
                }
              </app-select>
              <app-input
                [value]="keyLabel()"
                [placeholder]="'settings.providerKeys.labelPlaceholder' | transloco"
                [disabled]="settingKey()"
                (changed)="keyLabel.set($event)"
              />
            </div>
            <div class="form-row">
              <app-input
                type="password"
                [value]="keyValue()"
                [placeholder]="'settings.providerKeys.keyPlaceholder' | transloco"
                [disabled]="settingKey()"
                (changed)="keyValue.set($event)"
              />
            </div>
            <app-button
              variant="primary"
              size="md"
              [loading]="settingKey()"
              [disabled]="settingKey() || !keyValue().trim()"
              (clicked)="saveApiKey()"
            >
              {{
                settingKey()
                  ? ('common.saving' | transloco)
                  : ('settings.providerKeys.saveButton' | transloco)
              }}
            </app-button>
          </div>
        </section>

        <!-- Preferences Section -->
        <section class="settings-section section-spacer">
          <h2 class="section-title">{{ 'settings.preferences.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.preferences.desc' | transloco }}</p>

          <div class="form-block">
            <div class="form-row two-col">
              <app-form-field [label]="'settings.preferences.defaultModel' | transloco">
                <app-select
                  [value]="prefModel() ?? resolved().default_model ?? ''"
                  (changed)="onPrefChange(prefModel, resolved().default_model, $event)"
                >
                  @for (group of modelService.models(); track group.group) {
                    <optgroup
                      [label]="
                        group.configured
                          ? group.group
                          : group.group + ' ' + ('settings.preferences.noApiKey' | transloco)
                      "
                    >
                      @for (model of group.models; track model) {
                        <option [value]="model">
                          {{ model
                          }}{{
                            !prefModel() && model === resolved().default_model
                              ? ' (' + ('common.default' | transloco) + ')'
                              : ''
                          }}
                        </option>
                      }
                    </optgroup>
                  }
                </app-select>
              </app-form-field>
              <app-form-field [label]="'settings.preferences.auxModel' | transloco">
                <app-select
                  [value]="prefAuxModel() ?? resolved().default_auxiliary_model ?? ''"
                  (changed)="onPrefChange(prefAuxModel, resolved().default_auxiliary_model, $event)"
                >
                  @for (m of modelService.auxiliaryModels(); track m.id) {
                    <option [value]="m.id">
                      {{ m.label
                      }}{{
                        !prefAuxModel() && m.id === resolved().default_auxiliary_model
                          ? ' (' + ('common.default' | transloco) + ')'
                          : ''
                      }}{{ m.configured ? '' : ' ' + ('common.noKey' | transloco) }}
                    </option>
                  }
                </app-select>
              </app-form-field>
            </div>
            <div class="form-row two-col">
              <app-form-field [label]="'settings.preferences.autonomy' | transloco">
                <app-select
                  [value]="prefAutonomy() ?? resolved().default_autonomy ?? ''"
                  (changed)="onPrefChange(prefAutonomy, resolved().default_autonomy, $event)"
                >
                  <option value="full">
                    {{ 'settings.preferences.autonomyFull' | transloco
                    }}{{
                      !prefAutonomy() && resolved().default_autonomy === 'full'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                  <option value="review">
                    {{ 'settings.preferences.autonomyReview' | transloco
                    }}{{
                      !prefAutonomy() && resolved().default_autonomy === 'review'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                  <option value="partial">
                    {{ 'settings.preferences.autonomyPartial' | transloco
                    }}{{
                      !prefAutonomy() && resolved().default_autonomy === 'partial'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                  <option value="guided">
                    {{ 'settings.preferences.autonomyGuided' | transloco
                    }}{{
                      !prefAutonomy() && resolved().default_autonomy === 'guided'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                  <option value="dependent">
                    {{ 'settings.preferences.autonomyDependent' | transloco
                    }}{{
                      !prefAutonomy() && resolved().default_autonomy === 'dependent'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                </app-select>
              </app-form-field>
              <app-form-field [label]="'settings.preferences.reasoning' | transloco">
                <app-select
                  [value]="prefReasoning() ?? resolved().default_reasoning_level ?? ''"
                  (changed)="
                    onPrefChange(prefReasoning, resolved().default_reasoning_level, $event)
                  "
                >
                  <option value="low">
                    {{ 'settings.preferences.reasoningLow' | transloco
                    }}{{
                      !prefReasoning() && resolved().default_reasoning_level === 'low'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                  <option value="medium">
                    {{ 'settings.preferences.reasoningMedium' | transloco
                    }}{{
                      !prefReasoning() && resolved().default_reasoning_level === 'medium'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                  <option value="high">
                    {{ 'settings.preferences.reasoningHigh' | transloco
                    }}{{
                      !prefReasoning() && resolved().default_reasoning_level === 'high'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                </app-select>
              </app-form-field>
            </div>

            <h3 class="subsection-title">{{ 'settings.preferences.helperModels' | transloco }}</h3>
            <div class="form-row two-col">
              <app-form-field
                [label]="'settings.preferences.visionModel' | transloco"
                [hint]="'settings.preferences.visionHint' | transloco"
              >
                <app-select
                  [value]="prefVisionModel() ?? resolved().default_vision_model ?? ''"
                  (changed)="onPrefChange(prefVisionModel, resolved().default_vision_model, $event)"
                >
                  @for (m of modelService.visionModels(); track m.id) {
                    <option [value]="m.id">
                      {{ m.label
                      }}{{
                        !prefVisionModel() && m.id === resolved().default_vision_model
                          ? ' (' + ('common.default' | transloco) + ')'
                          : ''
                      }}{{ m.configured ? '' : ' ' + ('common.noKey' | transloco) }}
                    </option>
                  }
                </app-select>
              </app-form-field>
              <app-form-field
                [label]="'settings.preferences.whisperModel' | transloco"
                [hint]="'settings.preferences.whisperHint' | transloco"
              >
                <app-select
                  [value]="prefWhisperModel() ?? resolved().default_whisper_model ?? ''"
                  (changed)="
                    onPrefChange(prefWhisperModel, resolved().default_whisper_model, $event)
                  "
                >
                  @for (m of modelService.whisperModels(); track m.id) {
                    <option [value]="m.id">
                      {{ m.label
                      }}{{
                        !prefWhisperModel() && m.id === resolved().default_whisper_model
                          ? ' (' + ('common.default' | transloco) + ')'
                          : ''
                      }}{{ m.configured ? '' : ' ' + ('common.noKey' | transloco) }}
                    </option>
                  }
                </app-select>
              </app-form-field>
            </div>
            <div class="form-row two-col">
              <app-form-field
                [label]="'settings.preferences.embeddingModel' | transloco"
                [hint]="'settings.preferences.embeddingHint' | transloco"
              >
                <app-select
                  [value]="prefEmbeddingModel() ?? resolved().default_embedding_model ?? ''"
                  (changed)="
                    onPrefChange(prefEmbeddingModel, resolved().default_embedding_model, $event)
                  "
                >
                  @for (m of modelService.embeddingModels(); track m.id) {
                    <option [value]="m.id">
                      {{ m.label }}{{ m.dimensions ? ' (' + m.dimensions + 'd)' : ''
                      }}{{
                        !prefEmbeddingModel() && m.id === resolved().default_embedding_model
                          ? ' (' + ('common.default' | transloco) + ')'
                          : ''
                      }}{{ m.configured ? '' : ' ' + ('common.noKey' | transloco) }}
                    </option>
                  }
                </app-select>
              </app-form-field>
              <app-form-field
                [label]="'settings.preferences.embeddingProvider' | transloco"
                [hint]="'settings.preferences.embeddingProviderHint' | transloco"
              >
                <app-select
                  [value]="prefEmbeddingProvider() ?? resolved().embedding_provider ?? ''"
                  (changed)="
                    onPrefChange(prefEmbeddingProvider, resolved().embedding_provider, $event)
                  "
                >
                  <option value="local">
                    {{ 'settings.preferences.providerLocal' | transloco
                    }}{{
                      !prefEmbeddingProvider() && resolved().embedding_provider === 'local'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                  <option value="openrouter">
                    {{ 'settings.preferences.providerOpenrouter' | transloco
                    }}{{
                      !prefEmbeddingProvider() && resolved().embedding_provider === 'openrouter'
                        ? ' (' + ('common.default' | transloco) + ')'
                        : ''
                    }}
                  </option>
                </app-select>
              </app-form-field>
            </div>

            <div class="actions-row">
              <app-button
                variant="primary"
                size="md"
                [loading]="savingPrefs()"
                [disabled]="savingPrefs()"
                (clicked)="savePreferences()"
              >
                {{
                  savingPrefs()
                    ? ('common.saving' | transloco)
                    : ('settings.preferences.save' | transloco)
                }}
              </app-button>
              @if (prefsSaved()) {
                <app-badge tone="success" size="sm">{{ 'common.saved' | transloco }}</app-badge>
              }
            </div>
          </div>
        </section>

        <!-- DB-backed defaults: these choose the expert; the settings below
             remain fallback values for fields that expert does not specify. -->
        <section class="settings-section section-spacer">
          <h2 class="section-title">{{ 'settings.expertDefaults.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.expertDefaults.desc' | transloco }}</p>
          @if (expertDefaults(); as defaults) {
            @if (!defaults.personal_defaults_allowed) {
              <p class="section-hint">{{ 'settings.expertDefaults.restricted' | transloco }}</p>
            }
            <div class="form-block">
              @for (type of expertDefaultTypes; track type) {
                <div class="form-row two-col">
                  <app-form-field
                    [label]="'settings.expertDefaults.' + type | transloco"
                    [hint]="defaultExpertHint(type)"
                  >
                    <app-select
                      [value]="defaults.defaults[type].personal?.id ?? ''"
                      [disabled]="!defaults.personal_defaults_allowed || !!defaultExpertBusy()"
                      (changed)="setDefaultExpert(type, $event ?? '')"
                    >
                      <option value="">
                        {{ 'settings.expertDefaults.useApplication' | transloco }}
                      </option>
                      @for (expert of ownedExperts(type); track expert.id) {
                        <option [value]="expert.id">{{ expert.display_name }}</option>
                      }
                    </app-select>
                  </app-form-field>
                  <div class="actions-row default-expert-actions">
                    <app-button
                      variant="secondary"
                      size="sm"
                      [disabled]="!defaults.personal_defaults_allowed || !!defaultExpertBusy()"
                      [loading]="defaultExpertBusy() === type"
                      (clicked)="customizeDefaultExpert(type)"
                    >
                      {{ 'settings.expertDefaults.customize' | transloco }}
                    </app-button>
                    @if (defaults.defaults[type].personal && !defaults.personal_defaults_allowed) {
                      <app-button
                        variant="ghost"
                        size="sm"
                        [disabled]="!!defaultExpertBusy()"
                        (clicked)="setDefaultExpert(type, '')"
                      >
                        {{ 'settings.expertDefaults.clear' | transloco }}
                      </app-button>
                    }
                  </div>
                </div>
              }
              <div class="actions-row">
                <app-button variant="ghost" size="sm" (clicked)="openExperts()">
                  {{ 'settings.expertDefaults.manage' | transloco }}
                </app-button>
              </div>
            </div>
          }
        </section>

        <!-- Persistent Agent Section -->
        <section class="settings-section section-spacer">
          <h2 class="section-title">{{ 'settings.persistent.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.persistent.desc' | transloco }}</p>

          <div class="form-block">
            <app-form-field
              [label]="'settings.persistent.model' | transloco"
              [hint]="
                paModel()
                  ? ''
                  : resolved().persistent_agent?.model
                    ? ('settings.persistent.defaultPrefix' | transloco) +
                      ' ' +
                      (resolved().persistent_agent?.model || '')
                    : ''
              "
            >
              <app-input
                [value]="paModel() ?? ''"
                [placeholder]="
                  resolved().persistent_agent?.model ??
                  ('settings.persistent.modelPlaceholder' | transloco)
                "
                (changed)="onPaModelChange($event)"
              />
            </app-form-field>
            <app-form-field
              [label]="'settings.persistent.permissionMode' | transloco"
              [hint]="
                capabilities.permissionRestricted()
                  ? ('grants.locked.permission_mode' | transloco)
                  : ''
              "
            >
              <app-select
                [value]="paPermissionMode() ?? resolved().persistent_agent?.permission_mode ?? ''"
                (changed)="
                  onPrefChange(
                    paPermissionMode,
                    resolved().persistent_agent?.permission_mode,
                    $event
                  )
                "
              >
                <option value="supervised">
                  {{ 'settings.persistent.permissionSupervised' | transloco
                  }}{{
                    !paPermissionMode() &&
                    resolved().persistent_agent?.permission_mode === 'supervised'
                      ? ' (' + ('common.default' | transloco) + ')'
                      : ''
                  }}
                </option>
                <option
                  value="auto_accept"
                  [disabled]="!capabilities.allowsPermissionMode('auto_accept')"
                >
                  {{ 'settings.persistent.permissionAutoAccept' | transloco
                  }}{{
                    !paPermissionMode() &&
                    resolved().persistent_agent?.permission_mode === 'auto_accept'
                      ? ' (' + ('common.default' | transloco) + ')'
                      : ''
                  }}
                </option>
                <option
                  value="autonomous"
                  [disabled]="!capabilities.allowsPermissionMode('autonomous')"
                >
                  {{ 'settings.persistent.permissionAutonomous' | transloco
                  }}{{
                    !paPermissionMode() &&
                    resolved().persistent_agent?.permission_mode === 'autonomous'
                      ? ' (' + ('common.default' | transloco) + ')'
                      : ''
                  }}
                </option>
              </app-select>
            </app-form-field>
            <app-form-field
              [label]="'settings.persistent.workspaceBackend' | transloco"
              [hint]="'settings.persistent.workspaceBackendHint' | transloco"
            >
              <app-select
                [value]="
                  paWorkspaceBackend() ?? resolved().persistent_agent?.workspace_backend ?? ''
                "
                (changed)="
                  onPrefChange(
                    paWorkspaceBackend,
                    resolved().persistent_agent?.workspace_backend,
                    $event
                  )
                "
              >
                <option value="virtual">
                  {{ 'settings.persistent.workspaceVirtual' | transloco
                  }}{{
                    !paWorkspaceBackend() &&
                    resolved().persistent_agent?.workspace_backend === 'virtual'
                      ? ' (' + ('common.default' | transloco) + ')'
                      : ''
                  }}
                </option>
                <option value="sandbox">
                  {{ 'settings.persistent.workspaceSandbox' | transloco
                  }}{{
                    !paWorkspaceBackend() &&
                    resolved().persistent_agent?.workspace_backend === 'sandbox'
                      ? ' (' + ('common.default' | transloco) + ')'
                      : ''
                  }}
                </option>
                <option value="none">
                  {{ 'settings.persistent.workspaceNone' | transloco
                  }}{{
                    !paWorkspaceBackend() &&
                    resolved().persistent_agent?.workspace_backend === 'none'
                      ? ' (' + ('common.default' | transloco) + ')'
                      : ''
                  }}
                </option>
              </app-select>
            </app-form-field>
            <div class="form-row two-col">
              <app-form-field [label]="'settings.persistent.idleTimeout' | transloco">
                <app-input
                  type="number"
                  [value]="paIdleTimeoutText()"
                  [placeholder]="(resolved().persistent_agent?.idle_timeout_minutes ?? 30) + ''"
                  (changed)="onPaIdleTimeoutChange($event)"
                />
              </app-form-field>
              <app-form-field [label]="'settings.persistent.headlessMode' | transloco">
                <app-select
                  [value]="paHeadlessMode() ?? ''"
                  (changed)="paHeadlessMode.set($any($event || null))"
                >
                  <option value="">
                    {{ 'settings.persistent.headlessModeDefault' | transloco }}
                  </option>
                  <option value="eager">
                    {{ 'settings.persistent.headlessModeEager' | transloco }}
                  </option>
                  <option value="polite">
                    {{ 'settings.persistent.headlessModePolite' | transloco }}
                  </option>
                </app-select>
              </app-form-field>
            </div>
            <app-form-field
              [label]="'settings.persistent.attentionSleep' | transloco"
              [hint]="'settings.persistent.attentionSleepHint' | transloco"
            >
              <app-input
                type="number"
                [value]="paAttentionSleepText()"
                placeholder="60"
                (changed)="onPaAttentionSleepChange($event)"
              />
            </app-form-field>
            <div class="actions-row">
              <app-button
                variant="primary"
                size="md"
                [loading]="savingPA()"
                [disabled]="savingPA()"
                (clicked)="savePersistentAgent()"
              >
                {{
                  savingPA()
                    ? ('common.saving' | transloco)
                    : ('settings.persistent.save' | transloco)
                }}
              </app-button>
              @if (paSaved()) {
                <app-badge tone="success" size="sm">{{ 'common.saved' | transloco }}</app-badge>
              }
            </div>
          </div>
        </section>

        <!-- Communication Preferences Section -->
        <section class="settings-section section-spacer">
          <h2 class="section-title">{{ 'settings.communication.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.communication.desc' | transloco }}</p>

          <div class="form-block">
            <app-form-field [label]="'settings.communication.replyDelivery' | transloco">
              <app-select
                [value]="commDelivery()"
                (changed)="commDelivery.set($event ?? 'next_strategic_phase')"
              >
                <option value="next_strategic_phase">
                  {{ 'settings.communication.deliveryNextStrategic' | transloco }}
                </option>
                <option value="immediate_interrupt">
                  {{ 'settings.communication.deliveryImmediate' | transloco }}
                </option>
                <option value="llm_triage">
                  {{ 'settings.communication.deliveryLlmTriage' | transloco }}
                </option>
              </app-select>
            </app-form-field>

            <app-form-field [label]="'settings.communication.channels' | transloco">
              <div class="channel-list">
                <app-checkbox
                  size="sm"
                  [checked]="commChannelEmail()"
                  (changed)="commChannelEmail.set($event)"
                  >Email</app-checkbox
                >
                <app-checkbox
                  size="sm"
                  [checked]="commChannelNtfy()"
                  (changed)="commChannelNtfy.set($event)"
                  >Ntfy</app-checkbox
                >
                <app-checkbox
                  size="sm"
                  [checked]="commChannelSlack()"
                  (changed)="commChannelSlack.set($event)"
                  >Slack</app-checkbox
                >
                <app-checkbox
                  size="sm"
                  [checked]="commChannelDiscord()"
                  (changed)="commChannelDiscord.set($event)"
                  >Discord</app-checkbox
                >
              </div>
            </app-form-field>

            <app-checkbox [checked]="commQuietEnabled()" (changed)="commQuietEnabled.set($event)">
              {{ 'settings.communication.quietHours' | transloco }}
            </app-checkbox>

            @if (commQuietEnabled()) {
              <div class="form-row two-col quiet-hours-row">
                <app-form-field [label]="'settings.communication.start' | transloco">
                  <input
                    type="time"
                    class="time-input"
                    [value]="commQuietStart()"
                    (input)="commQuietStart.set(asInputValue($event))"
                  />
                </app-form-field>
                <app-form-field [label]="'settings.communication.end' | transloco">
                  <input
                    type="time"
                    class="time-input"
                    [value]="commQuietEnd()"
                    (input)="commQuietEnd.set(asInputValue($event))"
                  />
                </app-form-field>
              </div>
              <app-form-field [label]="'settings.communication.timezone' | transloco">
                <app-input
                  [value]="commQuietTimezone()"
                  [placeholder]="'settings.communication.timezonePlaceholder' | transloco"
                  (changed)="commQuietTimezone.set($event)"
                />
              </app-form-field>
            }

            <div class="actions-row">
              <app-button
                variant="primary"
                size="md"
                [loading]="savingComm()"
                [disabled]="savingComm()"
                (clicked)="saveCommunication()"
              >
                {{
                  savingComm()
                    ? ('common.saving' | transloco)
                    : ('settings.communication.save' | transloco)
                }}
              </app-button>
              @if (commSaved()) {
                <app-badge tone="success" size="sm">{{ 'common.saved' | transloco }}</app-badge>
              }
            </div>
          </div>
        </section>

        <!-- MCP Tokens Section -->
        @if (externalClientsEnabled) {
        <section class="settings-section section-spacer">
          <h2 class="section-title">{{ 'settings.mcp.title' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.mcp.desc' | transloco }}</p>

          <!-- Token List -->
          @if (tokenService.tokens().length > 0) {
            <div class="token-table">
              <div class="token-header">
                <span class="col-name">{{ 'settings.mcp.colName' | transloco }}</span>
                <span class="col-prefix">{{ 'settings.mcp.colToken' | transloco }}</span>
                <span class="col-scope">{{ 'settings.mcp.colScope' | transloco }}</span>
                <span class="col-origin">{{ 'settings.mcp.colOrigin' | transloco }}</span>
                <span class="col-used">{{ 'settings.mcp.colLastUsed' | transloco }}</span>
                <span class="col-expires">{{ 'settings.mcp.colExpires' | transloco }}</span>
                <span class="col-action"></span>
              </div>
              @for (token of activeTokens(); track token.id) {
                <div class="token-row">
                  <span class="col-name">{{ token.name }}</span>
                  <span class="col-prefix mono">{{ token.token_prefix }}...</span>
                  <span class="col-scope">{{ formatScope(token.scope) }}</span>
                  <span class="col-origin">{{ formatOrigin(token.origin) }}</span>
                  <span class="col-used">{{
                    token.last_used_at
                      ? formatDate(token.last_used_at)
                      : ('common.never' | transloco)
                  }}</span>
                  <span class="col-expires">{{
                    token.expires_at ? formatDate(token.expires_at) : ('common.never' | transloco)
                  }}</span>
                  <span class="col-action">
                    <app-button variant="danger" size="sm" (clicked)="revokeToken(token.id)">
                      {{ 'settings.mcp.revoke' | transloco }}
                    </app-button>
                  </span>
                </div>
              }
            </div>
          } @else {
            <p class="empty-state">{{ 'settings.mcp.empty' | transloco }}</p>
          }

          <!-- Newly Created Token -->
          @if (newToken(); as nt) {
            <div class="new-token-banner">
              <p class="new-token-warning">{{ 'settings.mcp.copyWarning' | transloco }}</p>
              <div class="new-token-row">
                <input
                  type="text"
                  class="new-token-input"
                  [value]="nt.token"
                  readonly
                  #tokenInput
                />
                <app-button variant="primary" size="md" (clicked)="copyToken(tokenInput)">
                  {{ copied() ? ('common.copied' | transloco) : ('common.copy' | transloco) }}
                </app-button>
              </div>
            </div>
          }

          <!-- Create Token Form -->
          <div class="create-form">
            <h3 class="form-title">{{ 'settings.mcp.createTitle' | transloco }}</h3>
            <div class="form-row">
              <app-input
                [value]="newName()"
                [placeholder]="'settings.mcp.namePlaceholder' | transloco"
                [disabled]="creating()"
                (changed)="newName.set($event)"
              />
            </div>
            <div class="form-row two-col">
              <app-select
                [value]="newScope()"
                [disabled]="creating()"
                (changed)="newScope.set($event ?? 'user')"
              >
                <option value="user">{{ 'settings.mcp.scopeUser' | transloco }}</option>
                @for (p of projects(); track p.id) {
                  <option [value]="'project:' + p.id">
                    {{ 'settings.mcp.scopeProjectPrefix' | transloco }} {{ p.name }}
                  </option>
                }
                @if (userService.currentUser()?.is_admin) {
                  <option value="all">{{ 'settings.mcp.scopeAll' | transloco }}</option>
                }
              </app-select>
              <app-select
                [value]="newExpiryText()"
                [disabled]="creating()"
                (changed)="onNewExpiryChange($event)"
              >
                <option value="">{{ 'settings.mcp.expiryNever' | transloco }}</option>
                <option value="30">{{ 'settings.mcp.expiry30' | transloco }}</option>
                <option value="90">{{ 'settings.mcp.expiry90' | transloco }}</option>
                <option value="365">{{ 'settings.mcp.expiry365' | transloco }}</option>
              </app-select>
            </div>
            @if (createError(); as err) {
              <p class="form-error" role="alert">{{ err }}</p>
            }
            <app-button
              variant="primary"
              size="md"
              [loading]="creating()"
              [disabled]="creating() || !newName().trim()"
              (clicked)="createToken()"
            >
              {{
                creating()
                  ? ('settings.mcp.creating' | transloco)
                  : ('settings.mcp.create' | transloco)
              }}
            </app-button>
          </div>

          <!-- Connection Instructions (shown after token creation) -->
          @if (newToken()) {
            <div class="instructions">
              <h3 class="form-title">{{ 'settings.mcp.claudeCodeTitle' | transloco }}</h3>
              <p class="section-desc" [innerHTML]="'settings.mcp.claudeCodeDesc' | transloco"></p>
              <div class="code-block-wrapper">
                <pre class="code-block">{{ mcpJsonSnippet() }}</pre>
                <app-button
                  class="code-copy-btn"
                  variant="primary"
                  size="sm"
                  (clicked)="copyText(mcpJsonSnippet())"
                >
                  {{
                    snippetCopied() ? ('common.copied' | transloco) : ('common.copy' | transloco)
                  }}
                </app-button>
              </div>
            </div>
          }

          <!-- Web UI Connector Instructions -->
          <div class="instructions">
            <h3 class="form-title">{{ 'settings.mcp.webConnectorTitle' | transloco }}</h3>
            <p class="section-desc">{{ 'settings.mcp.webConnectorDesc' | transloco }}</p>
            <div class="connector-url-row">
              <input
                type="text"
                class="readonly-input mono"
                [value]="mcpServerUrl()"
                readonly
                #mcpUrlInput
              />
              <app-button
                variant="primary"
                size="md"
                (clicked)="copyText(mcpUrlInput.value, 'connector')"
              >
                {{
                  connectorCopied() ? ('common.copied' | transloco) : ('common.copy' | transloco)
                }}
              </app-button>
            </div>
            <p class="section-hint">{{ 'settings.mcp.webConnectorHint' | transloco }}</p>
          </div>
        </section>

        }

        <!-- API Keys (PATs) link card — separate page per design doc §3.4 -->
        <section class="settings-section section-spacer">
          <h2 class="section-title">{{ 'settings.apiKeys.linkTitle' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.apiKeys.linkDesc' | transloco }}</p>
          <app-button variant="primary" size="md" (clicked)="goToApiKeys()">
            {{ 'settings.apiKeys.linkManage' | transloco }}
          </app-button>
        </section>

        <!-- SSH Keys link card — separate page, same pattern as the PAT card above -->
        @if (externalClientsEnabled) {
        <section class="settings-section section-spacer">
          <h2 class="section-title">{{ 'settings.sshKeys.linkTitle' | transloco }}</h2>
          <p class="section-desc">{{ 'settings.sshKeys.linkDesc' | transloco }}</p>
          <app-button variant="primary" size="md" (clicked)="goToSshKeys()">
            {{ 'settings.sshKeys.linkManage' | transloco }}
          </app-button>
        </section>

        }

        <!-- AI Subscriptions Section (Admin Only) -->
        @if (userService.currentUser()?.is_admin) {
          <section class="settings-section section-spacer">
            <h2 class="section-title">{{ 'settings.subscriptions.title' | transloco }}</h2>
            <p class="section-desc">{{ 'settings.subscriptions.desc' | transloco }}</p>

            <!-- Proxy reachability is reported separately from account state:
                 an unreachable proxy is "not enabled", not "signed out". -->
            <div class="subs-status-card">
              @if (subsLoading()) {
                <span class="subs-status-text">{{
                  'settings.subscriptions.checking' | transloco
                }}</span>
              } @else {
                <span class="subs-status-dot" [class.connected]="subsStatus().connected"></span>
                <span class="subs-status-text">
                  @if (!subsStatus().reachable) {
                    {{ 'settings.subscriptions.notEnabled' | transloco }}
                  } @else {
                    {{
                      (subsStatus().connected
                        ? 'settings.subscriptions.connected'
                        : 'settings.subscriptions.notConnected'
                      ) | transloco
                    }}
                    @if (subsStatus().model_count > 0) {
                      &mdash;
                      {{
                        'settings.subscriptions.modelsAvailable'
                          | transloco: {count: subsStatus().model_count}
                      }}
                    }
                  }
                </span>
                <app-button
                  variant="ghost"
                  size="sm"
                  [ariaLabel]="'settings.subscriptions.refreshStatus' | transloco"
                  (clicked)="loadSubscriptions()"
                >
                  <app-icon size="sm">refresh</app-icon>
                </app-button>
              }
            </div>

            <!-- Connected accounts -->
            @if (subsStatus().accounts.length > 0) {
              <div class="subs-accounts">
                @for (acct of subsStatus().accounts; track acct.account_id) {
                  <div class="subs-account-row">
                    <span class="subs-account-provider">{{ subscriptionProviderLabel(acct.provider) }}</span>
                    <span class="mono">{{ acct.email || acct.label || acct.account_id }}</span>
                    <span
                      class="subs-account-state"
                      [class.connected]="acct.state === 'connected'"
                      [class.warn]="acct.state === 'cooldown' || acct.state === 'disabled'"
                      [class.bad]="acct.state === 'error'"
                      [title]="acct.state_detail || ''"
                    >
                      {{ 'settings.subscriptions.states.' + acct.state | transloco }}
                    </span>
                    <span class="subs-account-scope">{{
                      'settings.subscriptions.scope.' + acct.scope | transloco
                    }}</span>
                    @if (usageFor(acct.account_id); as usage) {
                      @if (usage.available) {
                        <app-button
                          variant="ghost"
                          size="sm"
                          (clicked)="toggleUsage(acct.account_id)"
                        >
                          {{ 'settings.subscriptions.usage.toggle' | transloco }}
                        </app-button>
                      } @else {
                        <span class="subs-usage-na">{{
                          'settings.subscriptions.usage.unavailable' | transloco
                        }}</span>
                      }
                    } @else {
                      <app-button
                        variant="ghost"
                        size="sm"
                        (clicked)="loadUsage(acct.account_id)"
                      >
                        {{ 'settings.subscriptions.usage.check' | transloco }}
                      </app-button>
                    }
                    <app-button
                      variant="ghost"
                      size="sm"
                      (clicked)="disconnectAccount(acct.account_id)"
                    >
                      {{ 'settings.subscriptions.disconnect' | transloco }}
                    </app-button>
                  </div>

                  <!-- Usage bars: only for providers with a verified reader. -->
                  @if (expandedUsage() === acct.account_id && usageFor(acct.account_id); as usage) {
                    @if (usage.available) {
                      <div class="subs-usage">
                        <div class="subs-usage-title">
                          {{ 'settings.subscriptions.usage.title' | transloco }}
                          @if (usage.plan_type) {
                            <span class="subs-usage-plan">{{ usage.plan_type }}</span>
                          }
                          @if (usage.limit_reached) {
                            <span class="subs-usage-limit">{{
                              'settings.subscriptions.usage.limitReached' | transloco
                            }}</span>
                          }
                        </div>
                        @if (usage.primary; as w) {
                          <div class="subs-usage-row">
                            <div class="subs-usage-meta">
                              <span class="subs-usage-name">{{
                                'settings.subscriptions.usage.session' | transloco
                              }}</span>
                              @if (w.reset_after_seconds) {
                                <span class="subs-usage-reset">{{
                                  'settings.subscriptions.usage.resetsIn'
                                    | transloco: {time: formatResetIn(w.reset_after_seconds)}
                                }}</span>
                              }
                            </div>
                            <div class="subs-usage-track">
                              <div
                                class="subs-usage-fill"
                                [class]="usageTone(w.used_percent)"
                                [style.width.%]="w.used_percent ?? 0"
                              ></div>
                            </div>
                            <span class="subs-usage-pct">{{ w.used_percent ?? 0 }}%</span>
                          </div>
                        }
                        @if (usage.secondary; as w) {
                          <div class="subs-usage-row">
                            <div class="subs-usage-meta">
                              <span class="subs-usage-name">{{
                                'settings.subscriptions.usage.weekly' | transloco
                              }}</span>
                              @if (w.reset_after_seconds) {
                                <span class="subs-usage-reset">{{
                                  'settings.subscriptions.usage.resetsIn'
                                    | transloco: {time: formatResetIn(w.reset_after_seconds)}
                                }}</span>
                              }
                            </div>
                            <div class="subs-usage-track">
                              <div
                                class="subs-usage-fill"
                                [class]="usageTone(w.used_percent)"
                                [style.width.%]="w.used_percent ?? 0"
                              ></div>
                            </div>
                            <span class="subs-usage-pct">{{ w.used_percent ?? 0 }}%</span>
                          </div>
                        }
                        <p class="subs-usage-note">
                          {{ 'settings.subscriptions.usage.disclaimer' | transloco }}
                        </p>
                      </div>
                    }
                  }
                }
              </div>
            }

            @if (subsStatus().reachable) {
              <!-- Provider chooser -->
              <div class="subs-connect">
                <h3 class="form-title">{{ 'settings.subscriptions.addTitle' | transloco }}</h3>
                <div class="subs-provider-grid">
                  @for (p of subsStatus().providers; track p.key) {
                    <div class="subs-provider-card" [class.busy]="activeLogin()?.provider === p.key">
                      <div class="subs-provider-head">
                        <span class="subs-provider-label">{{ subscriptionProviderLabel(p.key) }}</span>
                        @if (p.connected_accounts > 0) {
                          <app-badge tone="success" size="xs">{{
                            'settings.subscriptions.connectedCount'
                              | transloco: {count: p.connected_accounts}
                          }}</app-badge>
                        }
                        @if (!p.inference_verified) {
                          <app-badge tone="warning" size="xs">{{
                            'settings.subscriptions.unverified' | transloco
                          }}</app-badge>
                        }
                      </div>
                      <p class="subs-provider-flow">
                        {{ 'settings.subscriptions.flows.' + p.login_flow | transloco }}
                      </p>
                      @for (note of p.notes; track note) {
                        <p class="subs-provider-note">{{ note }}</p>
                      }
                      <app-button
                        variant="secondary"
                        size="sm"
                        [loading]="activeLogin()?.provider === p.key && loginBusy()"
                        [disabled]="!!activeLogin() && activeLogin()?.provider !== p.key"
                        (clicked)="connectProvider(p.key)"
                      >
                        {{ 'settings.subscriptions.connect' | transloco }}
                      </app-button>
                    </div>
                  }
                </div>
              </div>

              <!-- In-flight authorization -->
              @if (activeLogin(); as login) {
                <div class="subs-login">
                  <p class="subs-login-title">
                    {{
                      'settings.subscriptions.login.' + login.status
                        | transloco: {provider: subscriptionProviderLabel(login.provider)}
                    }}
                  </p>

                  @if (login.status === 'pending' || login.status === 'verifying') {
                    @if (login.flow === 'device') {
                      <ol class="subs-login-steps">
                        <li>
                          {{ 'settings.subscriptions.device.step1' | transloco }}
                          <a [href]="login.auth_url" target="_blank" rel="noopener">{{
                            login.auth_url
                          }}</a>
                        </li>
                        @if (login.user_code) {
                          <li>
                            {{ 'settings.subscriptions.device.step2' | transloco }}
                            <code class="subs-user-code">{{ login.user_code }}</code>
                          </li>
                        }
                        <li>{{ 'settings.subscriptions.device.step3' | transloco }}</li>
                      </ol>
                      <p class="subs-login-hint">
                        {{
                          'settings.subscriptions.device.expires'
                            | transloco: {time: formatExpiry(login.expires_at)}
                        }}
                      </p>
                    } @else {
                      <ol class="subs-login-steps">
                        <li>
                          {{ 'settings.subscriptions.browser.step1' | transloco }}
                          <a [href]="login.auth_url" target="_blank" rel="noopener">{{
                            'settings.subscriptions.browser.openLink' | transloco
                          }}</a>
                        </li>
                        <li>{{ 'settings.subscriptions.browser.step2' | transloco }}</li>
                        <li>{{ 'settings.subscriptions.browser.step3' | transloco }}</li>
                      </ol>
                      <div class="subs-callback-row">
                        <app-input
                          [value]="callbackUrl()"
                          [placeholder]="
                            'settings.subscriptions.browser.callbackPlaceholder' | transloco
                          "
                          (changed)="callbackUrl.set($event)"
                        />
                        <app-button
                          variant="primary"
                          size="sm"
                          [loading]="callbackSubmitting()"
                          [disabled]="callbackSubmitting()"
                          (clicked)="submitCallback()"
                        >
                          {{ 'settings.subscriptions.browser.complete' | transloco }}
                        </app-button>
                      </div>
                      <p class="subs-login-hint">
                        {{ 'settings.subscriptions.browser.remoteHint' | transloco }}
                      </p>
                    }
                    <app-button variant="ghost" size="sm" (clicked)="cancelLogin()">
                      {{ 'settings.subscriptions.cancel' | transloco }}
                    </app-button>
                  } @else {
                    <app-button variant="ghost" size="sm" (clicked)="dismissLogin()">
                      {{ 'settings.subscriptions.dismiss' | transloco }}
                    </app-button>
                  }

                  @if (loginError()) {
                    <p class="subs-login-error">{{ loginError() }}</p>
                  }
                </div>
              }
            } @else if (!subsLoading()) {
              <!-- Proxy disabled: explain instead of offering a Connect button
                   that would 502 on the first login call. -->
              <div class="subs-disabled-notice">
                <p class="subs-disabled-title">
                  {{ 'settings.subscriptions.disabledTitle' | transloco }}
                </p>
                <p class="subs-disabled-desc">
                  {{ 'settings.subscriptions.disabledDesc' | transloco }}
                </p>
                <code class="subs-disabled-code">codexProxy.enabled: true</code>
              </div>
            }
          </section>

          <!-- Cloud Storage Section (Admin Only, Phase 4) -->
          <section class="settings-section section-spacer">
            <h2 class="section-title">{{ 'settings.cloud.title' | transloco }}</h2>
            <p class="section-desc">
              {{ 'settings.cloud.desc' | transloco }}
            </p>

            @if (cloudLoading()) {
              <p class="section-desc">{{ 'settings.cloud.loading' | transloco }}</p>
            } @else if (cloudSettings(); as s) {
              <!-- Status row -->
              <div class="subs-status-card">
                <span
                  class="subs-status-dot"
                  [class.connected]="s.effective.is_initialized"
                ></span>
                <span class="subs-status-text">
                  {{ 'settings.cloud.active' | transloco }}
                  <strong>{{ s.effective.backend_id }}</strong>
                  @if (s.effective.is_initialized) {
                    &mdash; {{ 'settings.cloud.initialized' | transloco }}
                  } @else {
                    &mdash; {{ 'settings.cloud.notInitialized' | transloco }}
                  }
                </span>
                <app-button
                  variant="ghost"
                  size="sm"
                  [ariaLabel]="'settings.cloud.refresh' | transloco"
                  (clicked)="loadCloudSettings()"
                >
                  <app-icon size="sm">refresh</app-icon>
                </app-button>
              </div>

              <!-- Backend selector -->
              <app-form-field [label]="'settings.cloud.backend' | transloco">
                <app-select
                  [value]="cloudForm().backend_id"
                  (changed)="updateCloudForm('backend_id', $event ?? '')"
                >
                  @for (backend of s.allowed_backends; track backend) {
                    <option [value]="backend">{{ backend }}</option>
                  }
                </app-select>
              </app-form-field>

              <!-- Common URL fields -->
              <app-form-field [label]="'settings.cloud.baseUrl' | transloco">
                <app-input
                  [value]="cloudForm().base_url || ''"
                  (changed)="updateCloudForm('base_url', $event)"
                />
              </app-form-field>
              <app-form-field [label]="'settings.cloud.publicUrl' | transloco">
                <app-input
                  [value]="cloudForm().public_url || ''"
                  (changed)="updateCloudForm('public_url', $event)"
                />
              </app-form-field>

              @if (cloudForm().backend_id === 'opencloud') {
                <app-form-field [label]="'settings.cloud.keycloakIssuer' | transloco">
                  <app-input
                    [value]="cloudForm().keycloak_issuer || ''"
                    (changed)="updateCloudForm('keycloak_issuer', $event)"
                  />
                </app-form-field>
                <app-form-field [label]="'settings.cloud.keycloakClientId' | transloco">
                  <app-input
                    [value]="cloudForm().keycloak_client_id || ''"
                    (changed)="updateCloudForm('keycloak_client_id', $event)"
                  />
                </app-form-field>
                <app-form-field [label]="'settings.cloud.adminRole' | transloco">
                  <app-input
                    [value]="cloudForm().admin_role_claim_value || ''"
                    (changed)="updateCloudForm('admin_role_claim_value', $event)"
                  />
                </app-form-field>
                <app-form-field [label]="'settings.cloud.spaceQuota' | transloco">
                  <app-input
                    type="number"
                    [value]="cloudQuotaText()"
                    (changed)="onCloudQuotaChange($event)"
                  />
                </app-form-field>
              }

              @if (cloudForm().backend_id === 'nextcloud') {
                <app-form-field [label]="'settings.cloud.adminUser' | transloco">
                  <app-input
                    [value]="cloudForm().admin_user || ''"
                    (changed)="updateCloudForm('admin_user', $event)"
                  />
                </app-form-field>
                <app-form-field [label]="'settings.cloud.agentUser' | transloco">
                  <app-input
                    [value]="cloudForm().agent_user || ''"
                    (changed)="updateCloudForm('agent_user', $event)"
                  />
                </app-form-field>
              }

              <!-- Credentials ref -->
              <app-form-field [label]="'settings.cloud.credentialsRef' | transloco">
                <app-input
                  [value]="cloudCredentialsRef()"
                  placeholder="env:OPENCLOUD_KEYCLOAK_CLIENT_SECRET"
                  (changed)="cloudCredentialsRef.set($event)"
                />
              </app-form-field>

              <!-- Secret provenance -->
              @if (secretProvenanceEntries().length > 0) {
                <div class="subs-accounts secret-provenance">
                  <h3 class="form-title">{{ 'settings.cloud.secretProvenance' | transloco }}</h3>
                  @for (entry of secretProvenanceEntries(); track entry.field) {
                    <div class="subs-account-row">
                      <span class="mono">{{ entry.field }}</span>
                      <span class="mono">{{ entry.env_var }}</span>
                      <span class="subs-account-state" [class.connected]="entry.set">
                        {{
                          entry.set
                            ? ('settings.cloud.secretSet' | transloco)
                            : ('settings.cloud.secretUnset' | transloco)
                        }}
                      </span>
                    </div>
                  }
                </div>
              }

              <!-- Buttons -->
              <div class="cloud-button-row">
                <app-button
                  variant="primary"
                  size="md"
                  [loading]="cloudTesting()"
                  [disabled]="cloudBusy()"
                  (clicked)="testCloudSettings()"
                >
                  {{
                    (cloudTesting() ? 'settings.cloud.testing' : 'settings.cloud.test') | transloco
                  }}
                </app-button>
                <app-button
                  variant="primary"
                  size="md"
                  [loading]="cloudSaving()"
                  [disabled]="cloudBusy()"
                  (clicked)="saveCloudSettings()"
                >
                  {{
                    (cloudSaving() ? 'settings.cloud.saving' : 'settings.cloud.saveReload')
                      | transloco
                  }}
                </app-button>
                @if (s.overlay.present) {
                  <app-button
                    variant="danger"
                    size="md"
                    [disabled]="cloudBusy()"
                    (clicked)="resetCloudSettings()"
                  >
                    {{ 'settings.cloud.resetEnv' | transloco }}
                  </app-button>
                }
              </div>

              @if (cloudMessage()) {
                <p
                  class="section-desc cloud-message"
                  [class.subs-login-error]="cloudMessageIsError()"
                >
                  {{ cloudMessage() }}
                </p>
              }

              @if (s.overlay.present) {
                <p class="section-desc cloud-overlay-info">
                  {{ 'settings.cloud.persistedOverlayLastSaved' | transloco }}
                  @if (s.overlay.updated_at) {
                    {{ formatDate(s.overlay.updated_at) }}
                  }
                  @if (s.overlay.updated_by) {
                    by {{ s.overlay.updated_by }}
                  }
                </p>
              }
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

      .settings-page {
        padding: 32px;
        max-width: var(--content-max-width);
        margin: 0 auto;
        color: var(--text-primary);
      }

      .page-header {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 32px;
      }

      .page-title {
        font-size: 24px;
        font-weight: 700;
        margin: 0;
        color: var(--text-primary);
      }

      .settings-section {
        background: var(--panel-bg);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-lg);
        padding: 24px;
      }

      /* Every card gets the same breathing room; .section-spacer used to be
         the only source of it, so the first three cards sat flush. */
      .settings-section + .settings-section,
      .section-spacer {
        margin-top: 24px;
      }

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

      .section-desc code {
        background: var(--surface-0);
        padding: 2px 6px;
        border-radius: var(--radius-tag);
        font-size: 12px;
      }

      /* Key + Token tables */
      .key-table,
      .token-table {
        margin-bottom: 20px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
        overflow: hidden;
      }

      .key-header,
      .key-row {
        display: grid;
        grid-template-columns: 1.5fr 1.2fr 1.2fr 1fr 90px;
        padding: 10px 14px;
        gap: 8px;
        align-items: center;
        font-size: 13px;
      }

      .token-header,
      .token-row {
        display: grid;
        grid-template-columns: 2fr 1.2fr 1fr 0.8fr 1fr 1fr 90px;
        padding: 10px 14px;
        gap: 8px;
        align-items: center;
        font-size: 13px;
      }

      .key-header,
      .token-header {
        background: var(--surface-0);
        font-weight: 600;
        font-size: 12px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .key-row,
      .token-row {
        border-top: 1px solid var(--border-color);
      }

      .mono {
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 12px;
        color: var(--text-muted);
      }

      .empty-state {
        font-size: 13px;
        color: var(--text-muted);
        text-align: center;
        padding: 24px;
        margin-bottom: 20px;
      }

      /* New token banner */
      .new-token-banner {
        background: var(--success-tint);
        border: 1px solid var(--success);
        border-radius: var(--radius-surface);
        padding: 14px;
        margin-bottom: 20px;
      }

      .new-token-warning {
        font-size: 13px;
        font-weight: 600;
        color: var(--success);
        margin-bottom: 10px;
      }

      .new-token-row {
        display: flex;
        gap: 8px;
      }

      .new-token-input,
      .readonly-input {
        flex: 1;
        padding: 8px 12px;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        color: var(--text-primary);
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 12px;
        outline: none;
      }

      /* Form layout */
      .form-block {
        display: flex;
        flex-direction: column;
        gap: 12px;
      }

      .create-form {
        border-top: 1px solid var(--border-color);
        padding-top: 20px;
        margin-bottom: 20px;
      }

      .form-title {
        font-size: 14px;
        font-weight: 600;
        margin-bottom: 12px;
        color: var(--text-primary);
      }

      .form-row {
        margin-bottom: 10px;
      }

      .two-col {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
      }

      .actions-row {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-top: 12px;
      }

      .channel-list {
        display: flex;
        gap: 16px;
        flex-wrap: wrap;
        margin-top: 4px;
      }

      .quiet-hours-row {
        margin-top: 12px;
      }

      .time-input {
        width: 100%;
        padding: 8px 12px;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        color: var(--text-primary);
        font-family: inherit;
        font-size: 13px;
        outline: none;
      }
      .time-input:focus {
        border-color: var(--accent-color);
      }

      .form-error {
        color: var(--danger);
        font-size: 12px;
        margin: 0 0 10px;
      }

      .subsection-title {
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
        margin: 16px 0 8px;
        padding-top: 12px;
        border-top: 1px solid var(--border-color);
      }

      /* Instructions */
      .instructions {
        border-top: 1px solid var(--border-color);
        padding-top: 20px;
      }

      .code-block-wrapper {
        position: relative;
      }

      .code-block {
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-tag);
        padding: 14px;
        padding-right: 80px;
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 12px;
        line-height: 1.5;
        color: var(--text-primary);
        overflow-x: auto;
        white-space: pre;
      }

      .code-copy-btn {
        position: absolute;
        top: 8px;
        right: 8px;
      }

      .connector-url-row {
        display: flex;
        gap: 8px;
        margin-bottom: 8px;
      }

      .section-hint {
        font-size: 12px;
        color: var(--text-secondary);
        line-height: 1.5;
      }

      /* AI Subscriptions (also reused by the Cloud Storage status/provenance
         rows — these are section-agnostic status primitives, not provider UI) */
      .subs-status-card {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 12px 16px;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
        margin-bottom: 16px;
      }

      .subs-status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--text-muted);
        flex-shrink: 0;
      }

      .subs-status-dot.connected {
        background: var(--color-success, #10b981);
      }

      .subs-status-text {
        font-size: 13px;
        color: var(--text-secondary);
        flex: 1;
      }

      .subs-disabled-notice {
        padding: 14px 16px;
        background: var(--surface-0);
        border: 1px dashed var(--border-color);
        border-radius: var(--radius-surface);
      }

      .subs-disabled-title {
        font-size: 13px;
        font-weight: 600;
        color: var(--text-primary);
        margin: 0 0 6px 0;
      }

      .subs-disabled-desc {
        font-size: 12px;
        color: var(--text-secondary);
        margin: 0 0 8px 0;
        line-height: 1.5;
      }

      .subs-disabled-code {
        display: inline-block;
        background: var(--surface-1);
        padding: 3px 8px;
        border-radius: var(--radius-tag);
        font-size: 11px;
      }

      .subs-accounts {
        display: flex;
        flex-direction: column;
        gap: 8px;
        margin-bottom: 16px;
      }

      .subs-account-row {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 10px 14px;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
        font-size: 13px;
      }

      .subs-account-row .mono {
        font-family: var(--font-mono);
        font-size: 12px;
        flex: 1;
      }

      .subs-account-provider {
        font-weight: 600;
        color: var(--text-primary);
      }

      .subs-account-state {
        font-size: 12px;
        color: var(--text-muted);
      }

      .subs-account-state.connected {
        color: var(--color-success, #10b981);
      }

      .subs-account-state.warn {
        color: var(--color-warning, #b45309);
      }

      .subs-account-state.bad {
        color: var(--color-danger, #dc2626);
      }

      .subs-account-scope {
        font-size: 11px;
        color: var(--text-muted);
      }

      .subs-usage-na {
        font-size: 11px;
        color: var(--text-muted);
      }

      /* Provider chooser */
      .subs-connect {
        margin-top: 8px;
      }

      .subs-provider-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
        gap: 12px;
        margin-bottom: 16px;
      }

      .subs-provider-card {
        display: flex;
        flex-direction: column;
        gap: 8px;
        padding: 14px;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
      }

      .subs-provider-card.busy {
        border-color: var(--color-accent, #6366f1);
      }

      .subs-provider-head {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
      }

      .subs-provider-label {
        font-size: 13px;
        font-weight: 600;
        color: var(--text-primary);
      }

      .subs-provider-flow,
      .subs-provider-note {
        font-size: 12px;
        color: var(--text-secondary);
        margin: 0;
        line-height: 1.5;
      }

      .subs-provider-note {
        color: var(--text-muted);
      }

      /* In-flight authorization */
      .subs-login {
        padding: 14px 16px;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
      }

      .subs-login-title {
        font-size: 13px;
        font-weight: 600;
        color: var(--text-primary);
        margin: 0 0 8px 0;
      }

      .subs-login-steps {
        margin: 0 0 10px 0;
        padding-left: 20px;
        font-size: 12px;
        color: var(--text-secondary);
        line-height: 1.7;
      }

      .subs-user-code {
        background: var(--surface-1);
        padding: 2px 8px;
        border-radius: var(--radius-tag);
        font-family: var(--font-mono);
        font-size: 13px;
      }

      .subs-callback-row {
        display: flex;
        gap: 8px;
        align-items: flex-start;
        margin-bottom: 8px;
      }

      .subs-callback-row > app-input {
        flex: 1;
      }

      .subs-login-hint {
        font-size: 12px;
        color: var(--text-muted);
        margin: 0 0 10px 0;
      }

      .subs-login-error {
        font-size: 12px;
        color: var(--color-danger, #dc2626);
        margin: 8px 0 0 0;
      }

      /* Usage bars */
      .subs-usage {
        padding: 12px 16px;
        margin: 0 0 8px 0;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
      }

      .subs-usage-title {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 12px;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 10px;
      }

      .subs-usage-plan {
        font-weight: 400;
        color: var(--text-muted);
        text-transform: capitalize;
      }

      .subs-usage-limit {
        color: var(--color-danger, #dc2626);
        font-weight: 600;
      }

      .subs-usage-row {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 8px;
      }

      .subs-usage-meta {
        display: flex;
        flex-direction: column;
        min-width: 120px;
      }

      .subs-usage-name {
        font-size: 12px;
        color: var(--text-secondary);
      }

      .subs-usage-reset {
        font-size: 11px;
        color: var(--text-muted);
      }

      .subs-usage-track {
        flex: 1;
        height: 6px;
        background: var(--surface-1);
        border-radius: var(--radius-tag);
        overflow: hidden;
      }

      .subs-usage-fill {
        height: 100%;
        border-radius: var(--radius-tag);
        transition: width 0.3s ease;
      }

      .subs-usage-fill.ok {
        background: var(--color-success, #10b981);
      }
      .subs-usage-fill.warn {
        background: var(--color-warning, #b45309);
      }
      .subs-usage-fill.crit {
        background: var(--color-danger, #dc2626);
      }

      .subs-usage-pct {
        font-size: 12px;
        color: var(--text-secondary);
        min-width: 36px;
        text-align: right;
      }

      .subs-usage-note {
        font-size: 11px;
        color: var(--text-muted);
        margin: 6px 0 0 0;
      }

      /* Cloud Storage */
      .cloud-button-row {
        display: flex;
        gap: 12px;
        margin-top: 20px;
        flex-wrap: wrap;
      }
      .cloud-message {
        margin-top: 12px;
      }
      .cloud-overlay-info {
        margin-top: 8px;
      }

      /* ---- Mobile (<=560px): this page's first responsive block ---- */
      @media (max-width: 560px) {
        /* Reclaim width — the 32px/24px desktop padding is wasteful on a phone. */
        .settings-page {
          padding: 16px;
        }
        .settings-section {
          padding: 16px;
        }

        /* Paired fields stack to one full-width column (bigger tap targets; the
         preference/persistent/cloud selects were cramped, not broken). */
        .two-col {
          grid-template-columns: 1fr;
        }

        /* API-key & MCP-token grids -> one card per row. A fixed 5-/7-column grid
         can't fit a phone, and the table wrapper is overflow:hidden, so the
         right-most column (the Delete / Revoke button) was clipped and
         UNREACHABLE. Cards restore every field + a full-width action button. */
        .key-table,
        .token-table {
          border: none;
          border-radius: 0;
          overflow: visible;
        }
        .key-header,
        .token-header {
          display: none;
        }
        .key-row,
        .token-row {
          display: block;
          border: 1px solid var(--border-color);
          border-radius: var(--radius-surface);
          padding: 12px 14px;
          margin-bottom: 10px;
        }
        .key-row > span,
        .token-row > span {
          display: block;
          padding: 1px 0;
        }
        /* First cell = card title. */
        .key-row .col-provider,
        .token-row .col-name {
          font-weight: 600;
          font-size: 14px;
          color: var(--text-primary);
          margin-bottom: 4px;
        }
        /* Re-label the now-headerless value cells. CSS content: is not scanned by
         the i18n hardcoded-string check and matches the admin-users card
         precedent. */
        .key-row .col-prefix::before {
          content: 'Key: ';
        }
        .key-row .col-label::before {
          content: 'Label: ';
        }
        .key-row .col-updated::before {
          content: 'Updated: ';
        }
        .token-row .col-prefix::before {
          content: 'Token: ';
        }
        .token-row .col-scope::before {
          content: 'Scope: ';
        }
        .token-row .col-origin::before {
          content: 'Origin: ';
        }
        .token-row .col-used::before {
          content: 'Last used: ';
        }
        .token-row .col-expires::before {
          content: 'Expires: ';
        }
        .key-row span::before,
        .token-row span::before {
          color: var(--text-muted);
          font-weight: 600;
        }
        /* Action cell -> full-width button at the foot of the card. */
        .key-row .col-action,
        .token-row .col-action {
          margin-top: 12px;
        }
        .key-row .col-action app-button,
        .token-row .col-action app-button {
          display: block;
          width: 100%;
        }
        .key-row .col-action ::ng-deep .app-button__btn,
        .token-row .col-action ::ng-deep .app-button__btn {
          width: 100%;
        }

        /* Subscription accounts & Cloud secret-provenance rows: let the long, unbreakable
         mono strings (e.g. OPENCLOUD_KEYCLOAK_CLIENT_SECRET) wrap instead of
         forcing the row -- and the whole page -- to scroll sideways. */
        .subs-account-row {
          flex-wrap: wrap;
        }
        .subs-account-row > * {
          min-width: 0;
        }
        .subs-account-row .mono {
          overflow-wrap: anywhere;
        }
      }
      .voice-lang-note {
        margin: 8px 0 0;
        font-size: 12px;
        line-height: 1.45;
        color: var(--text-muted);
      }
      .voice-sample-hint {
        margin: 4px 0 0;
        font-size: 12px;
        color: var(--text-muted);
      }
      .voice-preview-row {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-top: 10px;
      }
      .voice-preview-row app-icon {
        margin-right: 4px;
      }
      .voice-preview-error {
        font-size: 13px;
        color: var(--danger);
      }
      .voice-rewrite {
        margin-top: 14px;
        padding-top: 12px;
        border-top: 1px solid var(--border-color);
        display: flex;
        flex-direction: column;
        gap: 12px;
      }
      .voice-subhead {
        margin: 0;
        font-size: 13px;
        font-weight: 600;
        color: var(--text-primary);
      }
      .voice-library {
        margin-top: 14px;
        padding-top: 12px;
        border-top: 1px solid var(--border-color);
      }
      .voice-library-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        flex-wrap: wrap;
      }
      .voice-library-flag {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        font-size: 12px;
        color: var(--text-muted);
      }
      .voice-library-search {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-top: 12px;
        flex-wrap: wrap;
      }
      .voice-library-search app-input {
        flex: 1 1 180px;
      }
      .voice-library-results {
        display: flex;
        flex-direction: column;
        gap: 6px;
        margin-top: 12px;
      }
      .voice-library-card {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 8px 10px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        background: var(--surface-1);
      }
      .voice-library-card.is-added {
        border-color: var(--accent-color);
      }
      .voice-library-card__info {
        display: flex;
        flex-direction: column;
        gap: 2px;
        min-width: 0;
      }
      .voice-library-card__name {
        font-size: 13px;
        font-weight: 500;
        color: var(--text-primary);
      }
      .voice-library-card__tags {
        font-size: 12px;
        color: var(--text-muted);
        text-transform: capitalize;
      }
      .voice-library-card__actions {
        display: flex;
        align-items: center;
        gap: 6px;
        flex-shrink: 0;
      }
      .voice-library-card__done {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        font-size: 12px;
        color: var(--accent-color);
      }
    `,
  ],
})
export class SettingsComponent implements OnInit, OnDestroy {
  readonly externalClientsEnabled = environment.externalClientsEnabled;
  readonly tokenService = inject(McpTokenService);
  readonly userService = inject(UserService);
  readonly settingsService = inject(SettingsService);
  readonly modelService = inject(ModelService);
  readonly capabilities = inject(CapabilitiesService);
  readonly i18n = inject(I18nService);
  readonly viewMode = inject(ViewModeService);
  private readonly apiService = inject(ApiService);
  private readonly router = inject(Router);
  private readonly transloco = inject(TranslocoService);

  // Provider list for dropdown
  readonly providers = PROVIDERS;

  // ── Read-aloud voice ──────────────────────────────────────────────
  /** The TTS model in effect (user override or system default). The resolved
   * default lives on the service's `resolvedDefaults` signal — SettingsService
   * strips `_resolved` off `preferences` — so read it from there, not from
   * `preferences()._resolved` (which is always undefined). */
  readonly ttsModel = computed(() => {
    const p = this.settingsService.preferences();
    return p.default_tts_model || this.settingsService.resolvedDefaults().default_tts_model || '';
  });
  readonly ttsConfigured = computed(() => !!this.ttsModel());
  /** True when the user has pinned a TTS model (vs following the system
   * default) — drives the "(default)" annotation on the provider picker. */
  readonly ttsModelOverridden = computed(
    () => !!this.settingsService.preferences().default_tts_model,
  );
  /** Voices offered by the configured backend ([] ⇒ show a free-text field). */
  readonly ttsVoices = computed(() => voicesForModelId(this.ttsModel()));
  /** The configured TTS backend — drives the backend-specific note. */
  readonly ttsBackend = computed(() => ttsBackendForModelId(this.ttsModel()));
  /** The user's chosen voice ('' = follow the admin/per-language default). */
  readonly ttsVoice = computed(() => this.settingsService.preferences().default_tts_voice ?? '');

  /** Option label for a voice: the raw id plus a language tag when known
   * (e.g. `af_bella [EN-US]`). The `<option>` value stays the raw id. */
  voiceOptionLabel(voice: string): string {
    const tag = voiceLanguageTag(this.ttsModel(), voice);
    return tag ? `${voice} [${tag}]` : voice;
  }

  // ── ElevenLabs account voices ─────────────────────────────────────
  // Unlike Kokoro/OpenAI's static catalogs, ElevenLabs voices come live from
  // the deployment account (server-proxied `GET /api/settings/tts/voices`), so
  // they're an async signal loaded lazily when that backend is selected.
  readonly elevenVoices = signal<TtsAccountVoice[]>([]);
  readonly elevenVoicesLoading = signal(false);
  /** Fetched once per session; the orchestrator caches ~5 min so re-selecting
   * ElevenLabs doesn't re-hit the API. */
  private _elevenVoicesLoaded = false;
  private hostedAudio: HTMLAudioElement | null = null;

  /** The account voice matching the current selection (for the hosted-preview
   * button), or null when on Auto ('') or the voice isn't in the list. */
  readonly selectedElevenVoice = computed(
    () => this.elevenVoices().find((v) => v.id === this.ttsVoice()) ?? null,
  );

  /** Option label for an ElevenLabs voice: name + its own accent/gender labels
   * (e.g. `Sarah — american · female`) — strictly better than prefix-decoding.
   * The `<option>` value is the opaque account `voice_id`. */
  elevenVoiceLabel(v: TtsAccountVoice): string {
    const tags = [v.labels?.['accent'], v.labels?.['gender']].filter(Boolean).join(' · ');
    return tags ? `${v.name} — ${tags}` : v.name;
  }

  /** Play the selected voice's hosted `preview_url` (a public ElevenLabs CDN
   * mp3) — a zero-cost audition, distinct from the synth preview which spends
   * characters. No-op when the selected voice has no preview. */
  playHostedPreview(): void {
    const url = this.selectedElevenVoice()?.preview_url;
    if (!url) return;
    this.hostedAudio?.pause();
    const audio = new Audio(url);
    this.hostedAudio = audio;
    audio.play().catch(() => {
      /* autoplay/network failure — nothing actionable, stay silent */
    });
  }

  // ── Read-aloud rewrite preferences (reasoning + custom prompt) ────
  // Both feed the aux rewrite in services/tts.py. Draft signals + an explicit
  // Save (mirrors the persistent-agent / communication sub-object pattern) so
  // the free-text prompt isn't PATCHed on every keystroke; seeded from
  // preferences in the sync effect.
  readonly readAloudReasoning = signal<ReadAloudReasoningLevel>('off');
  readonly readAloudPromptDraft = signal('');
  /** Server caps the custom prompt (services/tts.py READ_ALOUD_PROMPT_MAX). */
  readonly readAloudPromptMax = 1000;
  readonly readAloudPromptCharsLeft = computed(
    () => this.readAloudPromptMax - this.readAloudPromptDraft().length,
  );
  readonly savingReadAloud = signal(false);
  readonly readAloudSaved = signal(false);

  /** Clamp typed/pasted instructions to the cap so we never submit a 422. */
  onReadAloudPromptChange(text: string): void {
    this.readAloudPromptDraft.set(text.slice(0, this.readAloudPromptMax));
  }

  /** Persist the read-aloud rewrite preferences as one sub-object. */
  saveReadAloud(): void {
    this.savingReadAloud.set(true);
    this.readAloudSaved.set(false);
    const settings: Record<string, unknown> = {
      read_aloud: {
        reasoning_level: this.readAloudReasoning(),
        custom_prompt: this.readAloudPromptDraft().trim() || null,
      },
    };
    this.settingsService.updatePreferences(settings).subscribe({
      next: () => {
        this.savingReadAloud.set(false);
        this.readAloudSaved.set(true);
        setTimeout(() => this.readAloudSaved.set(false), 2000);
      },
      error: () => this.savingReadAloud.set(false),
    });
  }

  // ── ElevenLabs Voice Library browser (Phase 6) ────────────────────
  // A thin skin over the server-proxied `/api/settings/tts/library` search.
  // Browsing/previewing is free; "Add to deployment" copies a voice into the
  // shared account and is gated by the admin flag (`libraryAddEnabled`).
  readonly libraryOpen = signal(false);
  readonly librarySearch = signal('');
  readonly libraryGender = signal('');
  readonly libraryLoading = signal(false);
  readonly libraryVoices = signal<TtsLibraryVoice[]>([]);
  readonly libraryError = signal<string | null>(null);
  /** Mirrors the deployment's add-gate; also reflected by the admin switch. */
  readonly libraryAddEnabled = signal(false);
  /** id of the library voice whose add is in-flight (drives its button spinner). */
  readonly libraryAddingId = signal<string | null>(null);
  /** id of the library voice just added, for a transient "Added ✓" affordance. */
  readonly libraryAdded = signal<string | null>(null);

  /** Open/close the library panel; first open kicks off an empty-query search
   * (the library's default "featured" listing). */
  toggleLibrary(): void {
    const open = !this.libraryOpen();
    this.libraryOpen.set(open);
    if (open && this.libraryVoices().length === 0 && !this.libraryLoading()) {
      this.searchLibrary();
    }
  }

  /** Run a library search with the current query + gender filter. */
  searchLibrary(): void {
    if (this.libraryLoading()) return;
    this.libraryLoading.set(true);
    this.libraryError.set(null);
    this.apiService
      .searchTtsLibrary({
        search: this.librarySearch(),
        gender: this.libraryGender(),
      })
      .subscribe((resp) => {
        this.libraryLoading.set(false);
        this.libraryAddEnabled.set(resp.add_enabled);
        this.libraryError.set(resp.error);
        this.libraryVoices.set(resp.voices);
      });
  }

  /** `accent · gender · language`, blanks dropped — the card's subtitle. */
  libraryVoiceLabel(v: TtsLibraryVoice): string {
    return [v.accent, v.gender, v.language].filter(Boolean).join(' · ');
  }

  /** Audition a library voice via its public CDN preview (zero characters). */
  playLibrarySample(v: TtsLibraryVoice): void {
    if (!v.preview_url) return;
    this.hostedAudio?.pause();
    const audio = new Audio(v.preview_url);
    this.hostedAudio = audio;
    audio.play().catch(() => {
      /* autoplay/network failure — stay silent */
    });
  }

  /** Copy a library voice into the deployment account, then refresh the account
   * picker (the server invalidated its cache) and select the new voice so the
   * user can immediately preview/use it. */
  addLibraryVoice(v: TtsLibraryVoice): void {
    if (this.libraryAddingId()) return;
    this.libraryAddingId.set(v.id);
    this.libraryError.set(null);
    this.apiService
      .addTtsLibraryVoice({
        public_owner_id: v.public_owner_id,
        voice_id: v.id,
        new_name: v.name,
      })
      .subscribe({
        next: (res) => {
          this.libraryAddingId.set(null);
          this.libraryAdded.set(v.id);
          const newId = res.voice_id || v.id;
          // Optimistically show + select the added voice NOW: the account list
          // is eventually-consistent, so an immediate bare refetch can miss the
          // just-added voice (that's the "Add did nothing until I toggled
          // provider" symptom). Insert a local entry built from the library card.
          const optimistic: TtsAccountVoice = {
            id: newId,
            name: v.name,
            labels: {
              ...(v.accent ? { accent: v.accent } : {}),
              ...(v.gender ? { gender: v.gender } : {}),
            },
            preview_url: v.preview_url,
          };
          if (!this.elevenVoices().some((x) => x.id === newId)) {
            this.elevenVoices.set([optimistic, ...this.elevenVoices()]);
          }
          this._elevenVoicesLoaded = true;
          this.setTtsVoice(newId);
          // Reconcile with the server in the background; if the fresh list still
          // lacks the voice (propagation lag), keep the optimistic entry.
          this.apiService.listTtsVoices().subscribe((r) => {
            if (r.backend !== 'elevenlabs' || r.voices.length === 0) return;
            this.elevenVoices.set(
              r.voices.some((x) => x.id === newId) ? r.voices : [optimistic, ...r.voices],
            );
          });
        },
        error: (err) => {
          this.libraryAddingId.set(null);
          this.libraryError.set((err?.error?.detail as string) || 'Could not add this voice.');
        },
      });
  }

  // ── Voice Library admin add-gate (admin-only switch) ──────────────
  readonly ttsLibraryFlag = signal(false);
  readonly ttsLibraryFlagSaving = signal(false);

  /** Toggle the deployment-wide "allow adding library voices" flag. */
  setTtsLibraryFlag(enabled: boolean): void {
    this.ttsLibraryFlagSaving.set(true);
    this.apiService.setTtsLibrarySetting(enabled).subscribe({
      next: (row) => {
        this.ttsLibraryFlag.set(row.enabled);
        this.libraryAddEnabled.set(row.enabled);
        this.ttsLibraryFlagSaving.set(false);
      },
      error: () => this.ttsLibraryFlagSaving.set(false),
    });
  }

  /** Persist the read-aloud provider/model choice. Selecting the resolved
   * default clears the override (mirrors the aux-model picker); any real change
   * also clears the voice override, because voice ids are backend-specific — a
   * Kokoro `af_*` id is meaningless to OpenAI/ElevenLabs, so carrying it over
   * would make the new backend reject the voice. */
  setTtsModel(modelId: string): void {
    if (modelId === this.ttsModel()) return;
    const resolvedDefault = this.settingsService.resolvedDefaults().default_tts_model ?? '';
    const override = !modelId || modelId === resolvedDefault ? null : modelId;
    this.settingsService
      .updatePreferences({ default_tts_model: override, default_tts_voice: null })
      .subscribe();
    this.previewErrorKey.set(null);
  }

  /** Persist the read-aloud voice choice (empty ⇒ clear the override). */
  setTtsVoice(voice: string): void {
    this.settingsService.updatePreferences({ default_tts_voice: voice || null }).subscribe();
    // A fresh selection invalidates any earlier preview error.
    this.previewErrorKey.set(null);
  }

  /** In-flight + last-error state for the "preview voice" button. Holds the
   * i18n key of the failure message (null = no error), so an actionable
   * provider error (paid-plan / auth / rate-limit) reads specifically instead
   * of a generic "couldn't preview". */
  readonly previewingVoice = signal(false);
  readonly previewErrorKey = signal<string | null>(null);
  private previewAudio: HTMLAudioElement | null = null;

  /** Map an actionable TTS error code to its message key (shared wording with
   * the read-aloud box). */
  private ttsErrorKey(code: string): string {
    switch (code) {
      case 'payment_required':
        return 'chat.tts.err.paymentRequired';
      case 'auth':
        return 'chat.tts.err.auth';
      case 'rate_limit':
        return 'chat.tts.err.rateLimit';
      default:
        return 'settings.voice.previewFailed';
    }
  }

  /** Optional custom sample text to audition the voice on (blank ⇒ canned
   * phrase). Mirrors the server's `_PREVIEW_TEXT_MAX`. */
  readonly previewTextMax = 500;
  readonly previewText = signal('');
  readonly previewCharsLeft = computed(() => this.previewTextMax - this.previewText().length);

  /** Clamp typed/pasted preview text to the cap so the UI can't submit
   * something the server would 422. */
  onPreviewTextChange(text: string): void {
    this.previewText.set(text.slice(0, this.previewTextMax));
  }

  /**
   * Synthesize and play a short sample of the currently-selected voice so the
   * user can hear it before committing. Empty voice ('' = Auto) is resolved
   * server-side exactly like normal read-aloud.
   */
  previewVoice(): void {
    if (this.previewingVoice()) return;
    // Stop any clip still playing from a previous press.
    this.previewAudio?.pause();
    this.previewAudio = null;
    this.previewErrorKey.set(null);
    this.previewingVoice.set(true);
    this.apiService
      .previewTTSVoice(this.ttsVoice(), this.i18n.activeLang(), this.previewText())
      .subscribe((result) => {
        this.previewingVoice.set(false);
        if (result === 'unavailable' || result === null) {
          this.previewErrorKey.set('settings.voice.previewFailed');
          return;
        }
        if ('errorCode' in result) {
          // e.g. a free-tier ElevenLabs Library voice → "needs a paid plan".
          this.previewErrorKey.set(this.ttsErrorKey(result.errorCode));
          return;
        }
        const url = URL.createObjectURL(result);
        const audio = new Audio(url);
        this.previewAudio = audio;
        audio.addEventListener('ended', () => URL.revokeObjectURL(url));
        // The click is a user gesture, so autoplay policy permits play();
        // guard anyway so a rejection surfaces as the error hint, not a throw.
        audio.play().catch(() => {
          this.previewErrorKey.set('settings.voice.previewFailed');
          URL.revokeObjectURL(url);
        });
      });
  }

  // MCP token form state
  readonly newName = signal('');
  readonly newScope = signal('user');
  readonly newExpiry = signal<number | null>(null);
  readonly newExpiryText = computed(() => {
    const v = this.newExpiry();
    return v == null ? '' : String(v);
  });
  readonly creating = signal(false);
  readonly createError = signal<string | null>(null);
  readonly newToken = signal<McpTokenCreateResponse | null>(null);
  readonly copied = signal(false);
  readonly snippetCopied = signal(false);
  readonly connectorCopied = signal(false);
  readonly projects = signal<Project[]>([]);

  // API key form state
  readonly keyProvider = signal<ApiKeyProvider>('openai');
  readonly keyValue = signal('');
  readonly keyLabel = signal('');
  readonly settingKey = signal(false);

  // Preferences form state — null = user hasn't overridden, use resolved default
  readonly prefModel = signal<string | null>(null);
  readonly prefAuxModel = signal<string | null>(null);
  readonly prefAutonomy = signal<string | null>(null);
  readonly prefReasoning = signal<string | null>(null);
  readonly prefVisionModel = signal<string | null>(null);
  readonly prefWhisperModel = signal<string | null>(null);
  readonly prefEmbeddingModel = signal<string | null>(null);
  readonly prefEmbeddingProvider = signal<string | null>(null);
  readonly savingPrefs = signal(false);
  readonly prefsSaved = signal(false);

  // The selected DB experts are independent from the preference fallbacks.
  readonly expertDefaultTypes = ['worker', 'session'] as const;
  readonly expertDefaults = signal<ExpertDefaultsResponse | null>(null);
  readonly experts = signal<Expert[]>([]);
  readonly defaultExpertBusy = signal<'worker' | 'session' | null>(null);

  /** Resolved defaults shortcut for template use. */
  readonly resolved = this.settingsService.resolvedDefaults;

  // Persistent Agent form state — null = use resolved default
  readonly paModel = signal<string | null>(null);
  readonly paPermissionMode = signal<string | null>(null);
  // Default session workspace tier (null = track the resolved system default,
  // which is "virtual" — see knowledge-base/knowledge/features/instant_landing_session.md).
  readonly paWorkspaceBackend = signal<string | null>(null);
  readonly paIdleTimeout = signal<number | null>(null);
  readonly paIdleTimeoutText = computed(() => {
    const v = this.paIdleTimeout();
    return v == null ? '' : String(v);
  });
  // Phase 6 headless controls. null = user has not overridden, fall back to
  // the framework default at the agent loader / sweeper layer.
  readonly paHeadlessMode = signal<'eager' | 'polite' | null>(null);
  readonly paAttentionSleepMinutes = signal<number | null>(null);
  readonly paAttentionSleepText = computed(() => {
    const v = this.paAttentionSleepMinutes();
    return v == null ? '' : String(v);
  });
  readonly savingPA = signal(false);
  readonly paSaved = signal(false);

  // Communication form state
  readonly commDelivery = signal<'next_strategic_phase' | 'immediate_interrupt' | 'llm_triage'>(
    'next_strategic_phase',
  );
  readonly commChannelEmail = signal(true);
  readonly commChannelNtfy = signal(false);
  readonly commChannelSlack = signal(false);
  readonly commChannelDiscord = signal(false);
  readonly commQuietEnabled = signal(false);
  readonly commQuietStart = signal('22:00');
  readonly commQuietEnd = signal('08:00');
  readonly commQuietTimezone = signal('');
  readonly savingComm = signal(false);
  readonly commSaved = signal(false);

  // AI subscriptions state (admin-only)
  readonly subsStatus = signal<SubscriptionsStatus>({
    reachable: false,
    connected: false,
    proxy_url: null,
    error: null,
    accounts: [],
    model_count: 0,
    providers: [],
  });
  readonly subsLoading = signal(false);
  /** The one in-flight authorization, if any. Only one runs at a time. */
  readonly activeLogin = signal<SubscriptionLogin | null>(null);
  readonly loginBusy = signal(false);
  readonly loginError = signal('');
  readonly callbackUrl = signal('');
  readonly callbackSubmitting = signal(false);
  /** Per-account usage, fetched on demand — never eagerly for every provider. */
  readonly accountUsage = signal<Record<string, SubscriptionUsage>>({});
  readonly expandedUsage = signal<string | null>(null);
  private loginPollTimer: ReturnType<typeof setInterval> | null = null;

  // Cloud storage state (admin-only, Phase 4)
  readonly cloudSettings = signal<MainCloudSettingsResponse | null>(null);
  readonly cloudLoading = signal(false);
  readonly cloudSaving = signal(false);
  readonly cloudTesting = signal(false);
  readonly cloudMessage = signal('');
  readonly cloudMessageIsError = signal(false);
  readonly cloudCredentialsRef = signal('');
  readonly cloudForm = signal<MainCloudFormState>({
    backend_id: 'opencloud',
    base_url: '',
    public_url: '',
    admin_user: '',
    agent_user: '',
    keycloak_issuer: '',
    keycloak_client_id: '',
    admin_role_claim_value: '',
    default_quota_bytes: null,
  });

  readonly cloudBusy = computed(() => this.cloudSaving() || this.cloudTesting());
  readonly cloudQuotaText = computed(() => {
    const v = this.cloudForm().default_quota_bytes;
    return v == null ? '' : String(v);
  });
  readonly secretProvenanceEntries = computed(() => {
    const s = this.cloudSettings();
    if (!s) return [];
    return Object.entries(s.secrets).map(([field, prov]) => ({
      field,
      env_var: prov.env_var,
      set: prov.set,
    }));
  });

  constructor() {
    // Reactively sync preference form fields when the preferences signal updates.
    // null = user hasn't overridden this field (show resolved default).
    effect(() => {
      const prefs = this.settingsService.preferences();
      if (Object.keys(prefs).length > 0) {
        this.prefModel.set(prefs.default_model ?? null);
        this.prefAuxModel.set(prefs.default_auxiliary_model ?? null);
        this.prefAutonomy.set(prefs.default_autonomy ?? null);
        this.prefReasoning.set(prefs.default_reasoning_level ?? null);
        this.prefVisionModel.set(prefs.default_vision_model ?? null);
        this.prefWhisperModel.set(prefs.default_whisper_model ?? null);
        this.prefEmbeddingModel.set(prefs.default_embedding_model ?? null);
        this.prefEmbeddingProvider.set(prefs.embedding_provider ?? null);

        // Seed the read-aloud rewrite draft (prefs only change on load/save, so
        // this never clobbers a mid-edit draft — same as the sub-objects below).
        const ra = prefs.read_aloud;
        this.readAloudReasoning.set((ra?.reasoning_level as ReadAloudReasoningLevel) ?? 'off');
        this.readAloudPromptDraft.set(ra?.custom_prompt ?? '');

        // Sync persistent agent preferences
        const pa = prefs.persistent_agent;
        if (pa) {
          this.paModel.set(pa.model ?? null);
          this.paPermissionMode.set(pa.permission_mode ?? null);
          this.paWorkspaceBackend.set(pa.workspace_backend ?? null);
          this.paIdleTimeout.set(pa.idle_timeout_minutes ?? null);
          this.paHeadlessMode.set(pa.headless_mode ?? null);
          this.paAttentionSleepMinutes.set(pa.headless_attention_sleep_minutes ?? null);
        }

        // Sync communication preferences
        const comm = prefs.communication;
        if (comm) {
          this.commDelivery.set(comm.delivery?.async_reply || 'next_strategic_phase');
          this.commChannelEmail.set(comm.channels?.email ?? true);
          this.commChannelNtfy.set(comm.channels?.ntfy ?? false);
          this.commChannelSlack.set(comm.channels?.slack_webhook ?? false);
          this.commChannelDiscord.set(comm.channels?.discord_webhook ?? false);
          this.commQuietEnabled.set(comm.quiet_hours?.enabled ?? false);
          this.commQuietStart.set(comm.quiet_hours?.start || '22:00');
          this.commQuietEnd.set(comm.quiet_hours?.end || '08:00');
          this.commQuietTimezone.set(comm.quiet_hours?.timezone || '');
        }
      }
    });

    // Load projects reactively — waits for currentUserId on F5 refresh
    effect(() => {
      const userId = this.userService.currentUserId();
      if (userId) {
        this.apiService.getProjects(userId).subscribe({
          next: (p) => this.projects.set(p),
          // Settings has many independent panels; the project-scoped ones
          // degrade to empty rather than failing the whole page.
          error: () => this.projects.set([]),
        });
      }
    });

    // Load admin-only sections reactively — waits for currentUser on F5
    // refresh, otherwise the panels render their headers but never fetch
    // (currentUser() is null when ngOnInit runs after a hard reload).
    // Guarded by `_adminLoadersFired` so subsequent user signal updates
    // (e.g. background refresh) don't re-trigger the fetches.
    effect(() => {
      if (this._adminLoadersFired) return;
      const user = this.userService.currentUser();
      if (user?.is_admin) {
        this._adminLoadersFired = true;
        this.loadSubscriptions();
        this.loadCloudSettings();
        // Seed the Voice Library add-gate switch with its persisted state.
        this.apiService.getTtsLibrarySetting().subscribe((row) => {
          this.ttsLibraryFlag.set(row.enabled);
          this.libraryAddEnabled.set(row.enabled);
        });
      }
    });

    // Lazily load ElevenLabs account voices the first time that backend is in
    // effect — the list is server-fed (names + accent labels + hosted
    // previews), not a static catalog like Kokoro/OpenAI. Depends only on
    // ttsBackend(); the one-shot flag keeps it from re-firing.
    effect(() => {
      if (this.ttsBackend() !== 'elevenlabs' || this._elevenVoicesLoaded) return;
      this._elevenVoicesLoaded = true;
      this.elevenVoicesLoading.set(true);
      this.apiService.listTtsVoices().subscribe((resp) => {
        this.elevenVoicesLoading.set(false);
        this.elevenVoices.set(resp.backend === 'elevenlabs' ? resp.voices : []);
      });
    });
  }

  private _adminLoadersFired = false;

  /** Only show active (non-revoked) tokens. */
  activeTokens = () => this.tokenService.tokens().filter((t) => !t.revoked_at);

  ngOnInit(): void {
    this.modelService.load();
    if (this.externalClientsEnabled) this.tokenService.loadTokens();
    this.settingsService.loadApiKeys();
    this.settingsService.loadPreferences();
    this.loadExpertDefaults();
    // Admin-only loaders (subscriptions + cloud settings) are triggered
    // by the effect in the constructor — that path waits for currentUser()
    // to populate, which is the only thing that works on a hard F5 reload.
  }

  ngOnDestroy(): void {
    // A login poll started here must not outlive the view.
    this.stopLoginPoll();
  }

  providerLabel(provider: string): string {
    return PROVIDERS.find((p) => p.value === provider)?.label || provider;
  }

  formatScope(scope: string): string {
    if (scope === 'user') return this.transloco.translate('settings.helpers.scopeMyData');
    if (scope === 'all') return this.transloco.translate('settings.helpers.scopeFullAccess');
    if (scope.startsWith('project:')) {
      const pid = scope.split(':', 2)[1];
      const p = this.projects().find((pr) => pr.id === pid);
      return p ? `Project: ${p.name}` : `Project`;
    }
    return scope;
  }

  formatOrigin(origin: string | null): string {
    if (!origin) return this.transloco.translate('settings.helpers.originManual');
    if (origin.startsWith('oauth:')) {
      const name = origin.slice(6).replace(/:refresh$/, '');
      return name.charAt(0).toUpperCase() + name.slice(1);
    }
    return origin;
  }

  formatDate(iso: string): string {
    const d = new Date(iso);
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    if (diffMs < 60_000) return this.transloco.translate('settings.helpers.timeJustNow');
    if (diffMs < 3600_000)
      return this.transloco.translate('settings.helpers.timeMinutesAgo', {
        n: Math.floor(diffMs / 60_000),
      });
    if (diffMs < 86400_000)
      return this.transloco.translate('settings.helpers.timeHoursAgo', {
        n: Math.floor(diffMs / 3600_000),
      });
    return d.toLocaleDateString(this.transloco.getActiveLang(), { month: 'short', day: 'numeric' });
  }

  goToApiKeys(): void {
    this.router.navigateByUrl('/settings/api-keys');
  }

  goToSshKeys(): void {
    this.router.navigateByUrl('/settings/ssh-keys');
  }

  mcpJsonSnippet = () => {
    if (!this.externalClientsEnabled) return '';
    const token = this.newToken()?.token ?? 'srw_YOUR_TOKEN_HERE';
    const mcpUrl = environment.mcpUrl;
    return JSON.stringify(
      {
        mcpServers: {
          orchestrator: {
            type: 'http',
            url: mcpUrl,
            headers: {
              Authorization: `Bearer ${token}`,
            },
          },
        },
      },
      null,
      2,
    );
  };

  mcpServerUrl = () => {
    return this.externalClientsEnabled ? environment.mcpUrl : '';
  };

  asInputValue(event: Event): string {
    return (event.target as HTMLInputElement).value;
  }

  copyText(text: string, target: string = 'snippet'): void {
    navigator.clipboard.writeText(text).then(() => {
      if (target === 'connector') {
        this.connectorCopied.set(true);
        setTimeout(() => this.connectorCopied.set(false), 2000);
      } else {
        this.snippetCopied.set(true);
        setTimeout(() => this.snippetCopied.set(false), 2000);
      }
    });
  }

  // ── Three-state default helpers ─────────────────────────────────

  /**
   * Update a "null = use resolved default" preference signal from a select change.
   * If the chosen value matches the resolved default, store null (so the user is
   * still tracking the framework default); otherwise store the override.
   */
  onPrefChange(
    target: { set: (v: string | null) => void },
    resolvedValue: string | undefined | null,
    next: string | null,
  ): void {
    if (next === null || next === '') {
      target.set(null);
      return;
    }
    target.set(next === resolvedValue ? null : next);
  }

  // ── API Keys ──────────────────────────────────────────────────────

  onKeyProviderChange(value: string | null): void {
    if (value) this.keyProvider.set(value as ApiKeyProvider);
  }

  saveApiKey(): void {
    const value = this.keyValue().trim();
    if (!value) return;
    this.settingKey.set(true);

    this.settingsService
      .setApiKey(this.keyProvider(), {
        api_key: value,
        label: this.keyLabel().trim() || undefined,
      })
      .subscribe({
        next: () => {
          this.keyValue.set('');
          this.keyLabel.set('');
          this.settingKey.set(false);
        },
        error: () => this.settingKey.set(false),
      });
  }

  deleteApiKey(provider: string): void {
    this.settingsService.deleteApiKey(provider).subscribe();
  }

  // ── Preferences ───────────────────────────────────────────────────

  savePreferences(): void {
    this.savingPrefs.set(true);
    this.prefsSaved.set(false);

    const settings: Record<string, string | null> = {};
    settings['default_model'] = this.prefModel()?.trim() || null;
    settings['default_auxiliary_model'] = this.prefAuxModel()?.trim() || null;
    settings['default_autonomy'] = this.prefAutonomy() || null;
    settings['default_reasoning_level'] = this.prefReasoning() || null;
    settings['default_vision_model'] = this.prefVisionModel() || null;
    settings['default_whisper_model'] = this.prefWhisperModel() || null;
    settings['default_embedding_model'] = this.prefEmbeddingModel() || null;
    settings['embedding_provider'] = this.prefEmbeddingProvider() || null;

    this.settingsService.updatePreferences(settings).subscribe({
      next: () => {
        this.savingPrefs.set(false);
        this.prefsSaved.set(true);
        setTimeout(() => this.prefsSaved.set(false), 2000);
      },
      error: () => this.savingPrefs.set(false),
    });
  }

  onLanguageChange(lang: SupportedLang): void {
    this.i18n.setLanguage(lang).subscribe();
  }

  // ── Persistent Agent Settings ──────────────────────────────────

  private loadExpertDefaults(): void {
    this.apiService.getExperts().subscribe((experts) => this.experts.set(experts));
    this.apiService.getExpertDefaults().subscribe((defaults) => {
      this.expertDefaults.set(defaults);
      this.defaultExpertBusy.set(null);
    });
  }

  ownedExperts(type: 'worker' | 'session'): Expert[] {
    const userId = this.userService.currentUserId();
    return this.experts().filter(
      (expert) =>
        expert.expert_type === type &&
        expert.storage_kind === 'db' &&
        !!userId &&
        expert.owner_id === userId,
    );
  }

  defaultExpertHint(type: 'worker' | 'session'): string {
    const slot = this.expertDefaults()?.defaults[type];
    if (!slot?.effective) return '';
    return this.transloco.translate('settings.expertDefaults.effective', {
      name: slot.effective.display_name,
      source: this.transloco.translate(`settings.expertDefaults.source.${slot.source}`),
    });
  }

  setDefaultExpert(type: 'worker' | 'session', expertId: string): void {
    if (this.defaultExpertBusy()) return;
    this.defaultExpertBusy.set(type);
    const request = expertId
      ? this.apiService.setPersonalExpertDefault(type, expertId)
      : this.apiService.clearPersonalExpertDefault(type);
    request.subscribe({
      next: () => this.loadExpertDefaults(),
      error: () => this.defaultExpertBusy.set(null),
    });
  }

  customizeDefaultExpert(type: 'worker' | 'session'): void {
    if (this.defaultExpertBusy()) return;
    const slot = this.expertDefaults()?.defaults[type];
    if (slot?.personal?.id) {
      this.router.navigate(['/experts', slot.personal.id, 'edit']);
      return;
    }
    const sourceId = slot?.effective?.id;
    this.defaultExpertBusy.set(type);
    this.apiService.forkPersonalExpertDefault(type, sourceId).subscribe({
      next: (result) => {
        const id = result.default?.id;
        this.loadExpertDefaults();
        // Carry `dropped` through the navigation (router state) rather than
        // a toast here: this page unmounts immediately on success, so a
        // banner on IT would flash and be gone before anyone could read it.
        // The editor the user lands on surfaces it once instead — see
        // `forkNoticeTranslationArgs` there.
        if (id) {
          const state: ExpertEditorNavigationState = { dropped: result.dropped };
          this.router.navigate(['/experts', id, 'edit'], { state });
        }
      },
      error: () => this.defaultExpertBusy.set(null),
    });
  }

  openExperts(): void {
    this.router.navigate(['/experts']);
  }

  onPaModelChange(text: string): void {
    this.paModel.set(text.trim() || null);
  }

  onPaIdleTimeoutChange(text: string): void {
    if (text === '' || text == null) {
      this.paIdleTimeout.set(null);
      return;
    }
    const n = Number(text);
    this.paIdleTimeout.set(Number.isFinite(n) ? n : null);
  }

  onPaAttentionSleepChange(text: string): void {
    if (text === '' || text == null) {
      this.paAttentionSleepMinutes.set(null);
      return;
    }
    const n = Number(text);
    this.paAttentionSleepMinutes.set(Number.isFinite(n) && n >= 0 ? n : null);
  }

  savePersistentAgent(): void {
    this.savingPA.set(true);
    this.paSaved.set(false);

    const settings: Record<string, unknown> = {
      persistent_agent: {
        model: this.paModel()?.trim() || null,
        permission_mode: this.paPermissionMode() || null,
        workspace_backend: this.paWorkspaceBackend() || null,
        idle_timeout_minutes: this.paIdleTimeout() || null,
        headless_mode: this.paHeadlessMode() || null,
        headless_attention_sleep_minutes: this.paAttentionSleepMinutes(),
      },
    };

    this.settingsService.updatePreferences(settings).subscribe({
      next: () => {
        this.savingPA.set(false);
        this.paSaved.set(true);
        setTimeout(() => this.paSaved.set(false), 2000);
      },
      error: () => this.savingPA.set(false),
    });
  }

  // ── Communication Settings ──────────────────────────────────────

  saveCommunication(): void {
    this.savingComm.set(true);
    this.commSaved.set(false);

    const communication: CommunicationSettings = {
      delivery: {
        async_reply: this.commDelivery(),
        urgent_override: true,
      },
      channels: {
        email: this.commChannelEmail(),
        cockpit: true,
        ntfy: this.commChannelNtfy(),
        slack_webhook: this.commChannelSlack(),
        discord_webhook: this.commChannelDiscord(),
      },
      quiet_hours: {
        enabled: this.commQuietEnabled(),
        start: this.commQuietStart(),
        end: this.commQuietEnd(),
        timezone: this.commQuietTimezone() || undefined,
      },
    };

    this.settingsService.updatePreferences({ communication }).subscribe({
      next: () => {
        this.savingComm.set(false);
        this.commSaved.set(true);
        setTimeout(() => this.commSaved.set(false), 2000);
      },
      error: () => this.savingComm.set(false),
    });
  }

  // ── MCP Tokens ────────────────────────────────────────────────────

  onNewExpiryChange(value: string | null): void {
    if (value === null || value === '') {
      this.newExpiry.set(null);
      return;
    }
    const n = Number(value);
    this.newExpiry.set(Number.isFinite(n) ? n : null);
  }

  createToken(): void {
    if (!this.externalClientsEnabled) return;
    const name = this.newName().trim();
    if (!name) return;
    this.creating.set(true);
    this.createError.set(null);
    this.newToken.set(null);
    this.copied.set(false);

    this.tokenService
      .createToken({
        name,
        scope: this.newScope(),
        expires_in_days: this.newExpiry(),
      })
      .subscribe({
        next: (res) => {
          this.newToken.set(res);
          this.newName.set('');
          this.creating.set(false);
        },
        error: (err) => {
          this.creating.set(false);
          const detail = (err?.error?.detail as string | undefined) ?? 'Create failed';
          this.createError.set(detail);
        },
      });
  }

  revokeToken(tokenId: string): void {
    this.tokenService.revokeToken(tokenId).subscribe();
  }

  copyToken(input: HTMLInputElement): void {
    navigator.clipboard.writeText(input.value).then(() => {
      this.copied.set(true);
      setTimeout(() => this.copied.set(false), 2000);
    });
  }

  // ── AI Subscriptions ────────────────────────────────────────
  //
  // The orchestrator owns the login sessions; the browser holds an SRW login
  // id and polls it. A poll reporting `verifying` is deliberately NOT rendered
  // as connected — upstream reporting "ok" is not proof that the credential
  // was persisted (see the service's poll_login).

  loadSubscriptions(): void {
    this.subsLoading.set(true);
    this.settingsService.getSubscriptionsStatus().subscribe((status) => {
      this.subsStatus.set(status);
      this.subsLoading.set(false);
    });
  }

  /** Translated product label, falling back to the server's English string. */
  subscriptionProviderLabel(key: string | null | undefined): string {
    if (!key) return '';
    const translated = this.transloco.translate(`settings.subscriptions.providers.${key}`);
    if (translated && translated !== `settings.subscriptions.providers.${key}`) {
      return translated;
    }
    return this.subsStatus().providers.find((p) => p.key === key)?.label ?? key;
  }

  usageFor(accountId: string): SubscriptionUsage | undefined {
    return this.accountUsage()[accountId];
  }

  loadUsage(accountId: string): void {
    this.settingsService.getSubscriptionUsage(accountId).subscribe((usage) => {
      this.accountUsage.update((current) => ({ ...current, [accountId]: usage }));
      if (usage.available) this.expandedUsage.set(accountId);
    });
  }

  toggleUsage(accountId: string): void {
    this.expandedUsage.update((current) => (current === accountId ? null : accountId));
  }

  /** Bar colour band by fill: ok (green) < 70 ≤ warn (amber) < 90 ≤ crit (red). */
  usageTone(percent: number | null | undefined): 'ok' | 'warn' | 'crit' {
    const value = typeof percent === 'number' ? percent : 0;
    if (value >= 90) return 'crit';
    if (value >= 70) return 'warn';
    return 'ok';
  }

  /** Human "2h 32m" / "3d 4h" / "12m" for a reset countdown (seconds). */
  formatResetIn(seconds: number | null | undefined): string {
    if (!seconds || seconds <= 0) return '';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    if (hours >= 24) return `${Math.floor(hours / 24)}d ${hours % 24}h`;
    if (hours >= 1) return `${hours}h ${minutes}m`;
    return `${minutes}m`;
  }

  /** Local-time expiry for a device-flow verification code. */
  formatExpiry(isoTimestamp: string): string {
    const at = new Date(isoTimestamp);
    return Number.isNaN(at.getTime()) ? '' : at.toLocaleTimeString();
  }

  connectProvider(providerKey: string): void {
    if (this.activeLogin()) return;
    this.loginBusy.set(true);
    this.loginError.set('');
    this.callbackUrl.set('');
    this.settingsService.startSubscriptionLogin(providerKey).subscribe({
      next: (login) => {
        this.loginBusy.set(false);
        this.activeLogin.set(login);
        // A browser flow needs the authorization page; a device flow shows the
        // verification URL + code inline instead of hijacking a tab.
        if (login.flow === 'browser') window.open(login.auth_url, '_blank');
        this.startLoginPoll(login.login_id);
      },
      error: (err) => {
        this.loginBusy.set(false);
        this.loginError.set(
          err?.error?.detail ?? this.transloco.translate('settings.subscriptions.errors.startFailed'),
        );
      },
    });
  }

  private startLoginPoll(loginId: string): void {
    this.stopLoginPoll();
    this.loginPollTimer = setInterval(() => {
      this.settingsService.pollSubscriptionLogin(loginId).subscribe({
        next: (login) => {
          this.activeLogin.set(login);
          if (login.status === 'connected') {
            this.stopLoginPoll();
            this.loadSubscriptions();
          } else if (login.status === 'failed' || login.status === 'cancelled') {
            this.stopLoginPoll();
            if (login.error) {
              this.loginError.set(
                this.translateLoginError(login.error),
              );
            }
          }
        },
        error: () => this.stopLoginPoll(),
      });
    }, 2000);
  }

  /** Map the service's stable error slugs to copy; pass anything else through. */
  private translateLoginError(code: string): string {
    const key = `settings.subscriptions.errors.${code}`;
    const translated = this.transloco.translate(key);
    return translated && translated !== key ? translated : code;
  }

  submitCallback(): void {
    const login = this.activeLogin();
    const url = this.callbackUrl().trim();
    if (!login || !url) return;
    this.loginError.set('');
    this.callbackSubmitting.set(true);
    this.settingsService.submitSubscriptionCallback(login.login_id, url).subscribe({
      next: (updated) => {
        this.callbackSubmitting.set(false);
        this.callbackUrl.set('');
        this.activeLogin.set(updated);
        if (updated.status === 'connected') {
          this.stopLoginPoll();
          this.loadSubscriptions();
        }
      },
      error: (err) => {
        this.callbackSubmitting.set(false);
        this.loginError.set(
          err?.error?.detail ??
            this.transloco.translate('settings.subscriptions.errors.completeFailed'),
        );
      },
    });
  }

  cancelLogin(): void {
    const login = this.activeLogin();
    if (!login) return;
    this.stopLoginPoll();
    this.settingsService.cancelSubscriptionLogin(login.login_id).subscribe({
      next: () => this.activeLogin.set(null),
      error: () => this.activeLogin.set(null),
    });
  }

  dismissLogin(): void {
    this.stopLoginPoll();
    this.activeLogin.set(null);
    this.loginError.set('');
  }

  disconnectAccount(accountId: string): void {
    this.settingsService.disconnectSubscriptionAccount(accountId).subscribe({
      next: () => {
        this.accountUsage.update((current) => {
          const next = { ...current };
          delete next[accountId];
          return next;
        });
        this.loadSubscriptions();
      },
      error: (err) => this.loginError.set(err?.error?.detail ?? ''),
    });
  }

  private stopLoginPoll(): void {
    if (this.loginPollTimer) {
      clearInterval(this.loginPollTimer);
      this.loginPollTimer = null;
    }
  }

  // ── Cloud Storage (Phase 4) ────────────────────────────────

  loadCloudSettings(): void {
    this.cloudLoading.set(true);
    this.cloudMessage.set('');
    this.cloudMessageIsError.set(false);
    this.settingsService.getMainCloudSettings().subscribe({
      next: (res) => {
        this.cloudSettings.set(res);
        // Seed the form from the persisted overlay if present; otherwise
        // from the effective config. Both sources are JSON-shaped, so we
        // bracket-access them through Record<> wrappers and coerce to the
        // typed MainCloudFormState shape.
        const overlayValue = (res.overlay?.value as Record<string, unknown>) || {};
        const eff = res.effective as unknown as Record<string, unknown>;
        const pickStr = (key: string): string => {
          const v = overlayValue[key] ?? eff[key];
          if (v === undefined || v === null) return '';
          return typeof v === 'string' ? v : String(v);
        };
        const pickQuota = (): number | null => {
          const v = overlayValue['default_quota_bytes'] ?? eff['default_quota_bytes'];
          if (v === undefined || v === null) return null;
          return typeof v === 'number' ? v : Number(v);
        };
        this.cloudForm.set({
          backend_id: res.effective.backend_id,
          base_url: pickStr('base_url'),
          public_url: pickStr('public_url'),
          admin_user: pickStr('admin_user'),
          agent_user: pickStr('agent_user'),
          keycloak_issuer: pickStr('keycloak_issuer'),
          keycloak_client_id: pickStr('keycloak_client_id'),
          admin_role_claim_value: pickStr('admin_role_claim_value'),
          default_quota_bytes: pickQuota(),
        });
        this.cloudCredentialsRef.set(res.overlay?.credentials_ref ?? '');
        this.cloudLoading.set(false);
      },
      error: (err) => {
        this.cloudLoading.set(false);
        this.cloudMessage.set(
          err?.error?.detail || this.transloco.translate('settings.cloud.messages.loadFailed'),
        );
        this.cloudMessageIsError.set(true);
      },
    });
  }

  updateCloudForm<K extends keyof MainCloudFormState>(key: K, value: MainCloudFormState[K]): void {
    this.cloudForm.update((form) => ({ ...form, [key]: value }));
  }

  onCloudQuotaChange(text: string): void {
    if (text === '' || text == null) {
      this.updateCloudForm('default_quota_bytes', null);
      return;
    }
    const n = Number(text);
    this.updateCloudForm('default_quota_bytes', Number.isFinite(n) ? n : null);
  }

  private buildCloudRequestBody() {
    const form = this.cloudForm();
    const credRef = this.cloudCredentialsRef().trim() || null;
    return {
      value: { ...form },
      credentials_ref: credRef,
      expected_activation_revision: this.cloudSettings()?.activation_revision ?? 0,
    };
  }

  testCloudSettings(): void {
    this.cloudTesting.set(true);
    this.cloudMessage.set('');
    this.cloudMessageIsError.set(false);
    this.settingsService.testMainCloudSettings(this.buildCloudRequestBody()).subscribe({
      next: (res) => {
        this.cloudTesting.set(false);
        this.cloudMessage.set(
          res.ok
            ? this.transloco.translate('settings.cloud.messages.testOk', {
                ms: res.latency_ms?.toFixed(0) ?? '?',
              })
            : this.transloco.translate('settings.cloud.messages.testFailed', { error: res.detail }),
        );
        this.cloudMessageIsError.set(!res.ok);
      },
      error: (err) => {
        this.cloudTesting.set(false);
        this.cloudMessage.set(
          err?.error?.detail ||
            this.transloco.translate('settings.cloud.messages.testRequestFailed'),
        );
        this.cloudMessageIsError.set(true);
      },
    });
  }

  saveCloudSettings(): void {
    this.cloudSaving.set(true);
    this.cloudMessage.set('');
    this.cloudMessageIsError.set(false);
    this.settingsService.putMainCloudSettings(this.buildCloudRequestBody()).subscribe({
      next: (res) => {
        this.cloudSaving.set(false);
        this.cloudMessage.set(
          this.transloco.translate(
            res.reloaded
              ? 'settings.cloud.messages.savedReloaded'
              : 'settings.cloud.messages.saved',
            { backend: res.backend_id },
          ),
        );
        this.cloudMessageIsError.set(false);
        this.loadCloudSettings();
      },
      error: (err) => {
        this.cloudSaving.set(false);
        this.cloudMessage.set(
          err?.error?.detail || this.transloco.translate('settings.cloud.messages.saveFailed'),
        );
        this.cloudMessageIsError.set(true);
      },
    });
  }

  resetCloudSettings(): void {
    this.cloudSaving.set(true);
    this.cloudMessage.set('');
    this.cloudMessageIsError.set(false);
    this.settingsService.deleteMainCloudSettings().subscribe({
      next: () => {
        this.cloudSaving.set(false);
        this.cloudMessage.set(this.transloco.translate('settings.cloud.messages.overlayCleared'));
        this.cloudMessageIsError.set(false);
        this.loadCloudSettings();
      },
      error: (err) => {
        this.cloudSaving.set(false);
        this.cloudMessage.set(
          err?.error?.detail || this.transloco.translate('settings.cloud.messages.resetFailed'),
        );
        this.cloudMessageIsError.set(true);
      },
    });
  }
}
