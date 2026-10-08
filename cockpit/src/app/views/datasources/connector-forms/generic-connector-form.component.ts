import {
  Component,
  computed,
  effect,
  EventEmitter,
  Input,
  linkedSignal,
  Output,
  signal,
} from '@angular/core';
import {NgTemplateOutlet} from '@angular/common';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppButtonComponent} from '../../../ui/button';
import {AppIconButtonComponent} from '../../../ui/icon-button';
import {AppIconComponent} from '../../../ui/icon';
import {AppInputComponent} from '../../../ui/input';
import {AppSelectComponent} from '../../../ui/select';
import {AppTextareaComponent} from '../../../ui/textarea';
import {ConnectorDriver} from '../../../core/models/connector-driver.model';
import {
  ChoiceState,
  ConnectorFormError,
  ExistingConnector,
  FieldState,
  FormNode,
  GenericFormValue,
  MapRow,
  ProblemReason,
  blankState,
  buildFormModel,
  childPath,
  errorAnchor,
  fieldsOf,
  formValue,
  initialFormState,
  renderedPointers,
  setStateAt,
  slotPointer,
  stateAt,
} from './schema-form';

/**
 * The generic connector form: a driver's `config_schema` and credential
 * slots rendered from its spec (connector_drivers.md, slice D2). The
 * connector editor uses it for any driver without a bespoke form
 * (connector-form-registry.ts); every built-in has one today, so it shows
 * for a built-in only through the editor's development preview.
 *
 * It owns the connection URL (built-in drivers only), `config` and
 * `credentials` of the request and emits them, with what still blocks a
 * submit, on every change. The editor submits; the API's refusal comes back
 * through `error` and shows at the field its JSON pointer names, or above
 * the form.
 */
@Component({
  selector: 'app-generic-connector-form',
  standalone: true,
  imports: [
    NgTemplateOutlet,
    TranslocoPipe,
    AppButtonComponent,
    AppIconButtonComponent,
    AppIconComponent,
    AppInputComponent,
    AppSelectComponent,
    AppTextareaComponent,
  ],
  template: `
    @if (model(); as form) {
    <div class="generic-form" [attr.data-driver]="driverSpec()?.name">
      @if (formError(); as message) {
        <div class="gf-error gf-form-error" role="alert">{{ message }}</div>
      }

      @if (form.connectionUrl; as rule) {
        <div class="gf-field" data-pointer="/connection_url">
          <span class="gf-label">
            {{ 'datasources.generic.connectionUrl' | transloco }}
            @if (rule === 'required' && !editMode()) { <span class="gf-required">*</span> }
          </span>
          <app-input
            size="sm"
            class="mono"
            [value]="text('connection_url')"
            (valueChange)="set('connection_url', $event, '/connection_url')"
            [placeholder]="editMode() ? ('datasources.generic.keepStored' | transloco) : ''"
            [disabled]="locked()"
            [ariaLabel]="'datasources.generic.connectionUrl' | transloco"
          />
          <ng-container *ngTemplateOutlet="messages; context: {pointer: '/connection_url'}" />
        </div>
      }

      @if (fieldsOf(form.config).length > 0) {
        <fieldset class="gf-section" data-section="config">
          <legend>{{ 'datasources.generic.config' | transloco }}</legend>
          <ng-container
            *ngTemplateOutlet="objectFields; context: {$implicit: form.config, path: 'config', pointer: '/config'}"
          />
        </fieldset>
      }

      <!-- The API stores an update's credentials whole (schema-form.ts formValue). -->
      @if (editMode() && form.slots.length > 0) {
        <div class="gf-hint gf-replace-hint" data-hint="replace-all">
          {{ 'datasources.generic.replaceAll' | transloco }}
        </div>
      }

      @for (entry of form.slots; track entry.slot.name) {
        <fieldset class="gf-section gf-slot" [attr.data-slot]="entry.slot.name">
          <legend>
            {{ entry.node.label }}
            <span class="gf-slot-kind">{{ 'datasources.generic.slotKind.' + entry.slot.kind | transloco }}</span>
            @if (entry.slot.required) { <span class="gf-required">*</span> }
          </legend>
          <div class="gf-hint">
            @if (entry.slot.access_levels.length > 0) {
              {{ 'datasources.generic.slotLevels' | transloco: {levels: entry.slot.access_levels.join(', ')} }}
            }
          </div>
          @for (field of fieldsOf(entry.node); track field.key) {
            <ng-container
              *ngTemplateOutlet="fieldTpl; context: {
                $implicit: field,
                path: child(child('slots', entry.slot.name), field.key),
                pointer: slotPointer(field),
                bare: false
              }"
            />
          }
        </fieldset>
      }
    </div>
    }

    <ng-template #objectFields let-node let-path="path" let-pointer="pointer">
      @for (group of node.groups; track $index) {
        <div class="gf-group" [attr.data-group]="group.name">
          @if (group.name) {
            <div class="gf-group-title">{{ group.name }}</div>
          }
          @for (field of group.fields; track field.key) {
            <ng-container
              *ngTemplateOutlet="fieldTpl; context: {
                $implicit: field,
                path: child(path, field.key),
                pointer: child(pointer, field.key),
                bare: false
              }"
            />
          }
        </div>
      }
    </ng-template>

    <ng-template #fieldTpl let-field let-path="path" let-pointer="pointer" let-bare="bare">
      @if (field.kind !== 'const') {
        <div
          class="gf-field"
          [class.gf-nested]="field.kind === 'object'"
          [attr.data-pointer]="bare ? null : pointer"
          [attr.data-kind]="field.kind"
        >
          @if (!bare && field.label && field.kind !== 'boolean' && field.kind !== 'object') {
            <span class="gf-label">
              {{ field.label }}
              @if (field.required && !(field.secret && editMode())) { <span class="gf-required">*</span> }
            </span>
          }
          @switch (field.kind) {
            @case ('object') {
              <fieldset class="gf-object">
                @if (!bare && field.label) {
                  <legend>
                    {{ field.label }}
                    @if (field.required) { <span class="gf-required">*</span> }
                  </legend>
                }
                <ng-container
                  *ngTemplateOutlet="objectFields; context: {$implicit: field, path: path, pointer: pointer}"
                />
              </fieldset>
            }
            @case ('boolean') {
              <label class="gf-check">
                <input
                  type="checkbox"
                  [checked]="read(path) === true"
                  (change)="set(path, $any($event.target).checked, pointer)"
                  [disabled]="locked()"
                >
                {{ field.label }}
              </label>
            }
            @case ('enum') {
              <app-select
                size="sm"
                [value]="text(path)"
                (changed)="set(path, $event ?? '', pointer)"
                [disabled]="locked()"
                [ariaLabel]="field.label"
              >
                <option value="">{{ 'datasources.generic.unset' | transloco }}</option>
                @for (option of field.options; track $index) {
                  <option [value]="'' + $index">{{ optionLabel(option) }}</option>
                }
              </app-select>
            }
            @case ('number') {
              <app-input
                size="sm"
                type="number"
                [value]="text(path)"
                (valueChange)="set(path, $event, pointer)"
                [disabled]="locked()"
                [ariaLabel]="field.label"
              />
            }
            @case ('text') {
              @if (field.widget === 'textarea' || field.widget === 'file') {
                <app-textarea
                  size="sm"
                  class="mono"
                  [rows]="4"
                  [value]="text(path)"
                  (valueChange)="set(path, $event, pointer)"
                  [placeholder]="field.secret && editMode() ? ('datasources.generic.keepStored' | transloco) : ''"
                  [disabled]="locked()"
                  [ariaLabel]="field.label"
                />
                @if (field.widget === 'file') {
                  <label class="gf-file">
                    <app-icon size="sm">upload_file</app-icon>
                    {{ 'datasources.generic.loadFile' | transloco }}
                    <input type="file" (change)="loadFile(path, pointer, $event)" [disabled]="locked()">
                  </label>
                }
              } @else {
                <app-input
                  size="sm"
                  [type]="field.widget === 'password' ? 'password' : 'text'"
                  [autocomplete]="field.widget === 'password' ? 'new-password' : ''"
                  [value]="text(path)"
                  (valueChange)="set(path, $event, pointer)"
                  [placeholder]="field.secret && editMode() ? ('datasources.generic.keepStored' | transloco) : ''"
                  [disabled]="locked()"
                  [ariaLabel]="field.label"
                />
              }
            }
            @case ('json') {
              <app-textarea
                size="sm"
                class="mono"
                [rows]="3"
                [value]="text(path)"
                (valueChange)="set(path, $event, pointer)"
                [placeholder]="field.secret && editMode() ? ('datasources.generic.keepStored' | transloco) : '{}'"
                [disabled]="locked()"
                [ariaLabel]="field.label"
              />
              <span class="gf-hint">{{ 'datasources.generic.jsonHint' | transloco }}</span>
            }
            @case ('list') {
              @for (item of items(path); track $index) {
                <div class="gf-row">
                  <div class="gf-row-body">
                    <ng-container
                      *ngTemplateOutlet="fieldTpl; context: {
                        $implicit: field.item,
                        path: child(path, $index),
                        pointer: child(pointer, $index),
                        bare: false
                      }"
                    />
                  </div>
                  <app-icon-button
                    variant="ghost"
                    size="sm"
                    [ariaLabel]="'datasources.generic.remove' | transloco"
                    [disabled]="locked()"
                    (clicked)="removeAt(path, $index, pointer)"
                  ><app-icon size="sm">close</app-icon></app-icon-button>
                </div>
              }
              <app-button
                variant="ghost"
                size="sm"
                class="gf-add"
                [disabled]="locked() || full(field, path)"
                (clicked)="append(path, field.item, pointer)"
              ><app-icon size="sm">add</app-icon> {{ 'datasources.generic.add' | transloco }}</app-button>
            }
            @case ('map') {
              @for (row of rows(path); track $index) {
                <div class="gf-row">
                  <app-input
                    size="sm"
                    class="mono gf-key"
                    [value]="row.key"
                    (valueChange)="set(child(child(path, $index), 'key'), $event, child(pointer, $event.trim()))"
                    [placeholder]="'datasources.generic.key' | transloco"
                    [disabled]="locked()"
                    [ariaLabel]="'datasources.generic.key' | transloco"
                  />
                  <div class="gf-row-body">
                    <ng-container
                      *ngTemplateOutlet="fieldTpl; context: {
                        $implicit: field.entry,
                        path: child(child(path, $index), 'value'),
                        pointer: child(pointer, row.key.trim()),
                        bare: true
                      }"
                    />
                    @if (row.key.trim()) {
                      <ng-container *ngTemplateOutlet="messages; context: {pointer: child(pointer, row.key.trim())}" />
                    }
                  </div>
                  <app-icon-button
                    variant="ghost"
                    size="sm"
                    [ariaLabel]="'datasources.generic.remove' | transloco"
                    [disabled]="locked()"
                    (clicked)="removeAt(path, $index, pointer)"
                  ><app-icon size="sm">close</app-icon></app-icon-button>
                </div>
              }
              <app-button
                variant="ghost"
                size="sm"
                class="gf-add"
                [disabled]="locked() || full(field, path)"
                (clicked)="appendRow(path, field.entry, pointer)"
              ><app-icon size="sm">add</app-icon> {{ 'datasources.generic.add' | transloco }}</app-button>
            }
            @case ('choice') {
              <app-select
                size="sm"
                [value]="'' + branchOf(path)"
                (changed)="pick(path, $event, pointer)"
                [disabled]="locked()"
                [ariaLabel]="field.label || ('datasources.generic.variant' | transloco)"
              >
                @for (branch of field.branches; track $index) {
                  <option [value]="'' + $index">{{ branch.label }}</option>
                }
              </app-select>
              <ng-container
                *ngTemplateOutlet="fieldTpl; context: {
                  $implicit: field.branches[branchOf(path)].node,
                  path: child(child(path, 'values'), branchOf(path)),
                  pointer: pointer,
                  bare: true
                }"
              />
            }
          }
          @if (!bare) {
            @if (field.description) {
              <span class="gf-hint">{{ field.description }}</span>
            }
            <ng-container *ngTemplateOutlet="messages; context: {pointer: pointer}" />
          }
        </div>
      }
    </ng-template>

    <ng-template #messages let-pointer="pointer">
      @if (apiErrorAt() === pointer && refusal()) {
        <span class="gf-error" role="alert">{{ refusal()!.message }}</span>
      } @else if (problemAt(pointer); as reason) {
        <span class="gf-error">{{ 'datasources.generic.problem.' + reason | transloco }}</span>
      }
    </ng-template>
  `,
  styles: `
    :host {
      display: block;
    }

    .generic-form {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .gf-section,
    .gf-object {
      display: flex;
      flex-direction: column;
      gap: 10px;
      min-width: 0;
      margin: 0;
      padding: 10px 12px;
      border: 1px solid var(--border-hairline);
      border-radius: var(--radius-surface);
    }

    .gf-object {
      padding: 8px 10px;
    }

    legend {
      padding: 0 4px;
      font-size: 12px;
      font-weight: 600;
      color: var(--text-secondary);
    }

    .gf-slot-kind {
      margin-left: 6px;
      font-weight: 400;
      color: var(--text-muted);
    }

    .gf-group {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .gf-group-title {
      font-size: 12px;
      font-weight: 600;
      color: var(--text-secondary);
    }

    .gf-field {
      display: flex;
      flex-direction: column;
      gap: 4px;
      min-width: 0;
    }

    .gf-label {
      font-size: 12px;
      font-weight: 500;
      color: var(--text-secondary);
    }

    .gf-required {
      margin-left: 2px;
      color: var(--danger);
    }

    .gf-hint {
      font-size: 11px;
      color: var(--text-muted);
    }

    .gf-hint:empty {
      display: none;
    }

    .gf-error {
      font-size: 12px;
      color: var(--danger);
    }

    .gf-form-error {
      padding: 8px 10px;
      border: 1px solid color-mix(in srgb, var(--danger) 40%, transparent);
      border-radius: var(--radius-surface);
      background: color-mix(in srgb, var(--danger) 8%, transparent);
    }

    .gf-check {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 13px;
    }

    .gf-row {
      display: flex;
      align-items: flex-start;
      gap: 6px;
    }

    .gf-row-body {
      flex: 1;
      min-width: 0;
    }

    .gf-key {
      flex: 0 0 38%;
      min-width: 0;
    }

    .gf-file {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      align-self: flex-start;
      font-size: 12px;
      color: var(--accent-color);
      cursor: pointer;
    }

    .gf-file input {
      position: absolute;
      width: 1px;
      height: 1px;
      opacity: 0;
    }

    .gf-add {
      align-self: flex-start;
    }

    .mono {
      font-family: var(--font-mono);
    }

    @media (max-width: 640px) {
      .gf-row {
        flex-wrap: wrap;
      }

      .gf-key {
        flex-basis: 100%;
      }
    }
  `,
})
export class GenericConnectorFormComponent {
  // Decorator inputs and output feeding signals: this repo's vitest JIT
  // harness wires neither signal inputs nor output() (see
  // helm-managed-badge.component.ts), and the spec drives them.
  protected readonly driverSpec = signal<ConnectorDriver | null>(null);
  protected readonly editMode = signal(false);
  protected readonly locked = signal(false);
  private readonly prefill = signal<ExistingConnector | null>(null);
  protected readonly refusal = signal<ConnectorFormError | null>(null);

  @Input({required: true}) set driver(value: ConnectorDriver) {
    this.driverSpec.set(value);
  }
  /** Editing a stored connector: a blank secret keeps the stored one. */
  @Input() set editing(value: boolean) {
    this.editMode.set(value);
  }
  @Input() set disabled(value: boolean) {
    this.locked.set(value);
  }
  /** What an edit prefills: the stored config and URL, never a secret. */
  @Input() set existing(value: ExistingConnector | null) {
    this.prefill.set(value);
  }
  /** The API's refusal of the last submit. */
  @Input() set error(value: ConnectorFormError | null) {
    this.refusal.set(value);
  }
  @Output() readonly valueChange = new EventEmitter<GenericFormValue>();

  readonly model = computed(() => {
    const driver = this.driverSpec();
    return driver ? buildFormModel(driver) : null;
  });
  /** Mutated in place; `version` tells the computeds it changed. */
  private readonly state = linkedSignal(() => {
    const model = this.model();
    return model ? initialFormState(model, this.prefill()) : null;
  });
  private readonly version = signal(0);
  /** Fields the user changed (see `problemAt`). */
  private readonly touched = linkedSignal(() => {
    this.state();
    return new Set<string>();
  });

  readonly value = computed(() => {
    this.version();
    const model = this.model();
    const state = this.state();
    return model && state ? formValue(model, state, this.editMode()) : null;
  });

  /** Where the API's error shows: the field it names, else above the form. */
  readonly apiErrorAt = computed(() => {
    const error = this.refusal();
    const model = this.model();
    const state = this.state();
    if (!error || !model || !state) return null;
    this.version();
    return errorAnchor(error.field, renderedPointers(model, state));
  });

  readonly formError = computed(() => {
    const error = this.refusal();
    return error && this.apiErrorAt() === null ? error.message : null;
  });

  protected readonly fieldsOf = fieldsOf;
  protected readonly slotPointer = slotPointer;
  protected readonly child = childPath;

  constructor() {
    effect(() => {
      const value = this.value();
      if (value) this.valueChange.emit(value);
    });
  }

  protected read(path: string): FieldState {
    this.version();
    return stateAt(this.state(), path);
  }

  protected text(path: string): string {
    const value = this.read(path);
    return typeof value === 'string' ? value : '';
  }

  protected items(path: string): FieldState[] {
    const value = this.read(path);
    return Array.isArray(value) ? (value as FieldState[]) : [];
  }

  protected rows(path: string): MapRow[] {
    const value = this.read(path);
    return Array.isArray(value) ? (value as MapRow[]) : [];
  }

  protected branchOf(path: string): number {
    return (this.read(path) as ChoiceState | null)?.branch ?? 0;
  }

  protected set(path: string, value: FieldState, pointer: string): void {
    setStateAt(this.state(), path, value);
    this.touch(pointer);
  }

  protected pick(path: string, value: string | null, pointer: string): void {
    setStateAt(this.state(), childPath(path, 'branch'), Number(value ?? 0));
    this.touch(pointer);
  }

  protected append(path: string, item: FormNode, pointer: string): void {
    this.items(path).push(blankState(item));
    this.touch(pointer);
  }

  protected appendRow(path: string, entry: FormNode, pointer: string): void {
    this.rows(path).push({key: '', value: blankState(entry)});
    this.touch(pointer);
  }

  protected removeAt(path: string, index: number, pointer: string): void {
    this.items(path).splice(index, 1);
    this.touch(pointer);
  }

  protected full(field: FormNode, path: string): boolean {
    return field.maxItems !== null && this.items(path).length >= field.maxItems;
  }

  /** Paste or upload: the file's text replaces the field's. */
  protected loadFile(path: string, pointer: string, event: Event): void {
    const element = event.target as HTMLInputElement;
    const file = element.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      this.set(path, String(reader.result ?? ''), pointer);
      element.value = '';
    };
    reader.readAsText(file);
  }

  protected optionLabel(option: unknown): string {
    return typeof option === 'string' ? option : JSON.stringify(option);
  }

  /** A field's local problem. A pristine form shows only the required
   *  markers; once anything is typed every problem shows, since they hold
   *  back Save (one typed credential on an edit asks for the others). */
  protected problemAt(pointer: string): ProblemReason | null {
    if (this.touched().size === 0) return null;
    return this.value()?.problems.find((problem) => problem.pointer === pointer)?.reason ?? null;
  }

  private touch(pointer: string): void {
    this.touched().add(pointer);
    this.version.update((v) => v + 1);
  }
}
