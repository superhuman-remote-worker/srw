import {ChangeDetectionStrategy, Component, computed, inject, OnInit} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AdminProvidersService, DEFAULT_MODEL_KINDS, DefaultModelKind, HelmManagedDefault} from '../../../core/services/admin-providers.service';
import {HelmManagedBadgeComponent} from '../../../ui/helm-managed-badge/helm-managed-badge.component';
import {ModelService} from '../../../core/services/model.service';
import {AppSelectComponent} from '../../../ui/select';
import {AppFormFieldComponent} from '../../../ui/form-field';

@Component({
  selector: 'app-admin-defaults',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    TranslocoPipe,
    HelmManagedBadgeComponent,
    AppSelectComponent,
    AppFormFieldComponent,
  ],
  template: `
    <div class="admin-defaults">
      <section class="admin-section">
        <h2 class="section-title">{{ 'admin.providers.defaults.title' | transloco }}</h2>
        <p class="section-desc">{{ 'admin.providers.defaults.desc' | transloco }}</p>

        @if (catalogEmpty()) {
          <p class="empty-state">{{ 'admin.providers.defaults.emptyCatalog' | transloco }}</p>
        } @else {
          <div class="defaults-form">
            @for (kind of defaultKinds; track kind) {
              <app-form-field
                [label]="('admin.providers.defaults.kind.' + kind) | transloco"
              >
                @if (helmFor(kind); as helm) {
                  <app-helm-managed-badge [managed]="helm.managed_by_helm" [drift]="helm.helm_drift" />
                }
                <app-select
                  [value]="admin.defaults()[kind] ?? ''"
                  (changed)="setDefault(kind, $event ?? '')"
                >
                  <option value="">
                    {{ (kind === 'search_fallback'
                      ? 'admin.providers.defaults.none'
                      : 'admin.providers.defaults.unset') | transloco }}
                  </option>
                  @if (kind === 'embedding') {
                    @for (m of modelService.embeddingModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else if (kind === 'rerank') {
                    @for (m of modelService.rerankModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else if (kind === 'vision') {
                    @for (m of modelService.visionModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else if (kind === 'whisper') {
                    @for (m of modelService.whisperModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else if (kind === 'tts') {
                    @for (m of modelService.ttsModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else if (kind === 'auxiliary') {
                    <!-- Strict: only rows whose capabilities[] includes
                         'auxiliary'. Under the array fan-out a chat row
                         registered as ['chat','auxiliary'] surfaces
                         here too — same row serves both slots. -->
                    @for (m of modelService.auxiliaryModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else if (kind === 'search' || kind === 'search_fallback') {
                    @for (m of modelService.searchModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else if (kind === 'fetch') {
                    @for (m of modelService.fetchModels(); track m.id) {
                      <option [value]="m.id">{{ m.label }}</option>
                    }
                  } @else {
                    <!-- Chat-slot kinds (chat/browser/citation):
                         strict filter by 'chat' capability via the
                         pre-bucketed groups list. -->
                    @for (group of modelService.models(); track group.group) {
                      <optgroup [label]="group.group">
                        @for (model of group.models; track model) {
                          <option [value]="model">{{ model }}</option>
                        }
                      </optgroup>
                    }
                  }
                </app-select>
                @if (helmFor(kind)?.auto_pinned && admin.defaults()[kind] === helmFor(kind)?.model) {
                  <p class="default-hint">{{ 'admin.providers.defaults.autoPinned' | transloco }}</p>
                }
                @if (kind === 'search_fallback' && searchFallbackMatchesPrimary()) {
                  <p class="default-warning">
                    {{ 'admin.providers.defaults.sameSearchWarning' | transloco }}
                  </p>
                }
              </app-form-field>
            }
          </div>
        }
      </section>
    </div>
  `,
  styles: [`
    :host {
      display: block;
    }
    .admin-defaults {
      display: block;
    }
    .admin-section {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 24px;
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
    .empty-state {
      font-size: 13px;
      color: var(--text-muted);
      text-align: center;
      padding: 18px 12px;
    }
    .defaults-form {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .default-warning {
      color: var(--color-warning, #b45309);
      font-size: 12px;
      margin: 6px 0 0;
    }
    .default-hint {
      color: var(--text-muted);
      font-size: 12px;
      margin: 6px 0 0;
    }
  `],
})
export class AdminDefaultsComponent implements OnInit {
  readonly admin = inject(AdminProvidersService);
  readonly modelService = inject(ModelService);
  private readonly transloco = inject(TranslocoService);

  readonly defaultKinds: DefaultModelKind[] = DEFAULT_MODEL_KINDS;
  readonly catalogEmpty = computed(() =>
    this.modelService.models().length === 0 &&
    this.modelService.auxiliaryModels().length === 0 &&
    this.modelService.embeddingModels().length === 0 &&
    this.modelService.visionModels().length === 0 &&
    this.modelService.whisperModels().length === 0 &&
    this.modelService.ttsModels().length === 0 &&
    this.modelService.searchModels().length === 0 &&
    this.modelService.fetchModels().length === 0 &&
    this.modelService.rerankModels().length === 0,
  );
  readonly searchFallbackMatchesPrimary = computed(() => {
    const primary = this.admin.defaults().search;
    return !!primary && primary === this.admin.defaults().search_fallback;
  });

  ngOnInit(): void {
    this.admin.loadDefaults();
    this.admin.loadHelmManaged();
    this.modelService.load();
  }

  /** Provenance of one pin from `GET /api/admin/helm-managed`, once loaded. */
  helmFor(kind: DefaultModelKind): HelmManagedDefault | null {
    return this.admin.helmManaged()?.defaults?.[kind] ?? null;
  }

  setDefault(kind: DefaultModelKind, model: string): void {
    if (this.helmFor(kind)?.managed_by_helm) {
      if (!confirm(this.transloco.translate('admin.helm.confirmOverride'))) {
        // Re-render the select with the still-current pin.
        this.admin.defaults.set({...this.admin.defaults()});
        return;
      }
    }
    this.admin.setDefault(kind, model).subscribe();
  }
}
