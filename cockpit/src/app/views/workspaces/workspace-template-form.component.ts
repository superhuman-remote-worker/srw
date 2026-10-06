import {Component, computed, inject, input, model} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppInputComponent} from '../../ui/input';
import {AppSelectComponent} from '../../ui/select';
import {AppTextareaComponent} from '../../ui/textarea';
import {ACCOUNT_ME, ResourceScope, WorkspaceBackend} from '../../core/models/workspace-template.model';
import {FormField, TemplateFormValue, emptyFormValue, isSrwImage, validateTemplateForm} from './workspace-template-utils';

export interface ScopeOption {
  key: string;
  scope: ResourceScope;
  label: string;
}

let nextFormId = 0;

/** One WorkspaceTemplate as a form (spec §3). Used by the editor page and the picker's Customize dialog. */
@Component({
  selector: 'app-workspace-template-form',
  standalone: true,
  imports: [TranslocoPipe, AppFormFieldComponent, AppInputComponent, AppSelectComponent, AppTextareaComponent],
  template: `
    <div class="form">
      @if (preservedNotice()) {
        <p class="notice">{{ 'workspaces.form.preserved' | transloco }}</p>
      }
      @if (purpose() === 'resource') {
        <div class="row">
          <app-form-field [forId]="id('name')" [label]="'workspaces.form.name' | transloco" [required]="true"
            [hint]="'workspaces.form.nameHint' | transloco" [error]="fieldError('name')">
            <app-input [inputId]="id('name')" [value]="value().name" [disabled]="readOnly() || identityLocked()"
              (valueChange)="patch({name: $event})" />
          </app-form-field>
          <app-form-field [forId]="id('saveTo')" [label]="'workspaces.form.saveTo' | transloco">
            <app-select [inputId]="id('saveTo')" [value]="scopeKey()" [disabled]="readOnly() || identityLocked()" (changed)="setScope($event)">
              @for (o of scopeOptions(); track o.key) {
                <option [value]="o.key">{{ o.label }}</option>
              }
            </app-select>
          </app-form-field>
        </div>
        <div class="row">
          <app-form-field [forId]="id('displayName')" [label]="'workspaces.form.displayName' | transloco">
            <app-input [inputId]="id('displayName')" [value]="value().displayName" [disabled]="readOnly()" (valueChange)="patch({displayName: $event})" />
          </app-form-field>
          <app-form-field [forId]="id('description')" [label]="'workspaces.form.description' | transloco">
            <app-input [inputId]="id('description')" [value]="value().description" [disabled]="readOnly()" (valueChange)="patch({description: $event})" />
          </app-form-field>
        </div>
      }
      <app-form-field [forId]="id('tier')" [label]="'workspaces.form.tier' | transloco" [error]="fieldError('backend')"
        [hint]="vmAllowed() ? '' : (vmUnavailableReasonKey() | transloco)">
        <app-select [inputId]="id('tier')" [value]="value().backend" [disabled]="readOnly()" (changed)="setBackend($event)">
          <option value="sandbox">{{ 'workspaces.tier.container' | transloco }}</option>
          <option value="vm" [disabled]="!vmAllowed()">{{ 'workspaces.tier.vm' | transloco }}</option>
          <option value="virtual">{{ 'workspaces.tier.virtual' | transloco }}</option>
        </app-select>
      </app-form-field>
      @if (value().backend !== 'virtual') {
        <app-form-field [forId]="id('image')" [label]="'workspaces.form.image' | transloco" [required]="true"
          [hint]="'workspaces.form.imageHint' | transloco" [error]="fieldError('image')">
          <app-input [inputId]="id('image')" [value]="value().image" [list]="listId" [disabled]="readOnly()" (valueChange)="patch({image: $event})" />
          <datalist [id]="listId">
            @for (i of suggestedImages(); track i) {
              <option [value]="i"></option>
            }
          </datalist>
        </app-form-field>
        @if (customImage()) {
          <div class="notice custom" role="note">
            <strong>{{ 'workspaces.customImage.title' | transloco }}</strong>
            <ul>
              <li>{{ 'workspaces.customImage.unprivileged' | transloco }}</li>
              <li>{{ 'workspaces.customImage.digest' | transloco }}</li>
              <li>{{ 'workspaces.customImage.jobFirst' | transloco }}</li>
            </ul>
          </div>
        }
        <div class="row three">
          <app-form-field [forId]="id('cpu')" [label]="'workspaces.form.cpu' | transloco" [error]="fieldError('cpu')">
            <app-input [inputId]="id('cpu')" [value]="value().cpu" inputmode="decimal" [disabled]="readOnly()" (valueChange)="patch({cpu: $event})" />
          </app-form-field>
          <app-form-field [forId]="id('memory')" [label]="'workspaces.form.memory' | transloco" [hint]="'workspaces.form.quantityHint' | transloco" [error]="fieldError('memory')">
            <app-input [inputId]="id('memory')" [value]="value().memory" placeholder="4Gi" [disabled]="readOnly()" (valueChange)="patch({memory: $event})" />
          </app-form-field>
          <app-form-field [forId]="id('storage')" [label]="'workspaces.form.storage' | transloco" [hint]="'workspaces.form.quantityHint' | transloco" [error]="fieldError('storage')">
            <app-input [inputId]="id('storage')" [value]="value().storage" placeholder="20Gi" [disabled]="readOnly()" (valueChange)="patch({storage: $event})" />
          </app-form-field>
        </div>
        <details class="advanced">
          <summary>{{ 'workspaces.form.advanced' | transloco }}</summary>
          @if (value().backend === 'sandbox') {
            <div class="row">
              <app-form-field [forId]="id('requestCpu')" [label]="'workspaces.form.requestCpu' | transloco" [hint]="'workspaces.form.requestHint' | transloco" [error]="fieldError('requestCpu')">
                <app-input [inputId]="id('requestCpu')" [value]="value().requestCpu" inputmode="decimal" [disabled]="readOnly()" (valueChange)="patch({requestCpu: $event})" />
              </app-form-field>
              <app-form-field [forId]="id('requestMemory')" [label]="'workspaces.form.requestMemory' | transloco" [error]="fieldError('requestMemory')">
                <app-input [inputId]="id('requestMemory')" [value]="value().requestMemory" placeholder="1Gi" [disabled]="readOnly()" (valueChange)="patch({requestMemory: $event})" />
              </app-form-field>
            </div>
            <app-form-field [forId]="id('pullPolicy')" [label]="'workspaces.form.pullPolicy' | transloco" [error]="fieldError('pullPolicy')">
              <app-select [inputId]="id('pullPolicy')" [value]="value().pullPolicy" [disabled]="readOnly()" (changed)="patch({pullPolicy: pullPolicyOf($event)})">
                <option value="">{{ 'workspaces.form.pullPolicyDefault' | transloco }}</option>
                <option value="IfNotPresent">IfNotPresent</option>
                <option value="Always">Always</option>
                <option value="Never">Never</option>
              </app-select>
            </app-form-field>
          }
          @if (value().backend === 'vm') {
            <app-form-field [forId]="id('setupLines')" [label]="'workspaces.form.setupSteps' | transloco" [hint]="'workspaces.form.setupStepsHint' | transloco" [error]="fieldError('setupLines')">
              <app-textarea [inputId]="id('setupLines')" [value]="value().setupLines" [rows]="5" [disabled]="readOnly()" (valueChange)="patch({setupLines: $event})" />
            </app-form-field>
          }
        </details>
      }
    </div>
  `,
  styles: [`
    :host { display: block; }
    .form { display: flex; flex-direction: column; gap: 0.75rem; }
    .row { display: flex; gap: 1rem; align-items: flex-start; }
    .row > * { flex: 1; min-width: 0; }
    .notice { margin: 0; padding: 0.5rem 0.75rem; border-radius: var(--radius-control); background: var(--info-tint); color: var(--text-primary); font-size: 0.85rem; }
    .notice.custom ul { margin: 0.25rem 0 0; padding-left: 1.25rem; }
    .advanced summary { cursor: pointer; color: var(--text-secondary); font-size: 0.85rem; margin-bottom: 0.5rem; }
    @media (max-width: 768px) { .row { flex-direction: column; } }
  `],
})
export class WorkspaceTemplateFormComponent {
  private readonly transloco = inject(TranslocoService);

  readonly value = model<TemplateFormValue>(emptyFormValue(ACCOUNT_ME));
  readonly purpose = input<'resource' | 'inline'>('resource');
  readonly readOnly = input(false);
  readonly identityLocked = input(false);
  readonly scopeOptions = input<ScopeOption[]>([]);
  readonly vmAllowed = input(true);
  readonly vmUnavailableReasonKey = input('');
  readonly srwImages = input<Partial<Record<WorkspaceBackend, string[]>>>({});
  readonly preservedNotice = input(false);
  readonly serverErrors = input<Partial<Record<FormField, string>>>({});
  readonly showErrors = input(false);

  /** One id per field, unique per form instance, so each label can point at its control. */
  protected id(field: string): string {
    return `${this.listId}-${field}`;
  }

  protected readonly listId = `srw-workspace-images-${nextFormId++}`;
  readonly errors = computed(() => validateTemplateForm(this.value()));
  readonly valid = computed(() => Object.keys(this.errors()).length === 0);
  readonly suggestedImages = computed(() => this.srwImages()[this.value().backend] ?? []);
  readonly customImage = computed(() => {
    const v = this.value();
    return v.backend !== 'virtual' && v.image.trim() !== '' && this.suggestedImages().length > 0 && !isSrwImage(v.image, this.suggestedImages());
  });
  protected readonly scopeKey = computed(() => `${this.value().scope.kind}/${this.value().scope.name}`);

  patch(change: Partial<TemplateFormValue>): void {
    this.value.update((v) => ({...v, ...change}));
  }

  setBackend(backend: string | null): void {
    if (backend === 'vm' && !this.vmAllowed()) return;
    if (backend === 'sandbox' || backend === 'vm' || backend === 'virtual') this.patch({backend});
  }

  setScope(key: string | null): void {
    const option = this.scopeOptions().find((o) => o.key === key);
    if (option) this.patch({scope: {...option.scope}});
  }

  protected pullPolicyOf(v: string | null): TemplateFormValue['pullPolicy'] {
    return v === 'IfNotPresent' || v === 'Always' || v === 'Never' ? v : '';
  }

  /** A server message always shows. A client check shows once the user tried to save. */
  fieldError(field: FormField): string {
    const server = this.serverErrors()[field];
    if (server) return server;
    const key = this.errors()[field];
    return key && this.showErrors() ? this.transloco.translate(key) : '';
  }
}
