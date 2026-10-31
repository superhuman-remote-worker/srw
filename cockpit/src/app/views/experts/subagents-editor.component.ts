import {Component, computed, inject, input, output, signal} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {TranslocoPipe} from '@jsverse/transloco';
import {forkJoin, map} from 'rxjs';
import {ApiService} from '../../core/services/api.service';
import {chatModelLabel} from '../../core/services/model.service';
import {
  SUBAGENT_INHERIT_MODEL,
  type Expert,
  type SubagentIsolation,
  type SubagentRosterEntry,
  type SubagentsConfig,
  type SubagentWritePolicy,
} from '../../core/models/api.model';
import {AppButtonComponent} from '../../ui/button';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppIconComponent} from '../../ui/icon';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppInputComponent} from '../../ui/input';
import {AppSelectComponent} from '../../ui/select';
import {AppSwitchComponent} from '../../ui/switch';
import {AppTextareaComponent} from '../../ui/textarea';

/** `subagents.roster` key grammar (config/schema.json `propertyNames`). */
export const ROSTER_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

export type RosterEntryKind = 'inline' | 'reference';

const ISOLATIONS: readonly SubagentIsolation[] = ['shared', 'worktree'];
const WRITE_POLICIES: readonly SubagentWritePolicy[] = ['none', 'scratch_only', 'owned_paths', 'full'];

/** The entry keys the structured controls own; everything else lives in the
 *  "other keys" JSON textarea (U1 — tools/limits/return/prompts grow controls
 *  of their own in U3). */
const STRUCTURED_KEYS: ReadonlySet<string> = new Set([
  '$ref',
  'description',
  'llm',
  'isolation',
  'write_policy',
]);

/** Editor state for one roster entry. */
export interface RosterEntryDraft {
  /** Stable identity for `@for` — names are editable. */
  key: number;
  name: string;
  kind: RosterEntryKind;
  /** Reference kind: the `$ref` spelling (`critic`, `subagents/explorer`, a DB id). */
  ref: string;
  description: string;
  /** '' = inherit the parent's model (inline) / no override (reference). */
  model: string;
  isolation: '' | SubagentIsolation;
  writePolicy: '' | SubagentWritePolicy;
  /** JSON of every other entry key (tools, limits, return, prompts, the rest
   *  of `llm`, …), kept verbatim so a hand-authored entry round-trips. */
  extraText: string;
}

/** A catalog group as `ModelService.models` serves it. */
export interface ModelCatalogGroup {
  group: string;
  models: string[];
  labels?: Record<string, string>;
}

export interface ParsedExtra {
  value?: Record<string, unknown>;
  error?: 'invalid' | 'notObject';
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Parse the "other keys" textarea: blank ⇒ `{}`; must be a JSON object. */
export function parseExtra(text: string): ParsedExtra {
  const t = text.trim();
  if (!t) return {value: {}};
  let parsed: unknown;
  try {
    parsed = JSON.parse(t);
  } catch {
    return {error: 'invalid'};
  }
  if (!isPlainObject(parsed)) return {error: 'notObject'};
  return {value: parsed};
}

/** Split a stored roster entry into the editor's draft fields (pure). */
export function draftFromEntry(name: string, entry: SubagentRosterEntry, key: number): RosterEntryDraft {
  const llm: Record<string, unknown> = isPlainObject(entry.llm) ? {...entry.llm} : {};
  const pinned = llm['model'];
  const model = typeof pinned === 'string' && pinned !== SUBAGENT_INHERIT_MODEL ? pinned : '';
  delete llm['model'];

  const extra: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(entry)) {
    if (!STRUCTURED_KEYS.has(k)) extra[k] = v;
  }
  if (Object.keys(llm).length) extra['llm'] = llm;

  const ref = typeof entry.$ref === 'string' ? entry.$ref : '';
  return {
    key,
    name,
    kind: ref ? 'reference' : 'inline',
    ref,
    description: typeof entry.description === 'string' ? entry.description : '',
    model,
    isolation: ISOLATIONS.includes(entry.isolation as SubagentIsolation) ? (entry.isolation as SubagentIsolation) : '',
    writePolicy: WRITE_POLICIES.includes(entry.write_policy as SubagentWritePolicy)
      ? (entry.write_policy as SubagentWritePolicy)
      : '',
    extraText: Object.keys(extra).length ? JSON.stringify(extra, null, 2) : '',
  };
}

/**
 * Rebuild the stored entry from a draft (pure). `extra` is the parsed
 * "other keys" object; the structured fields win over a key of the same name
 * typed there, and `llm.model` is merged into whatever else `llm` carries.
 * A reference entry is `{$ref, ...overrides}`; an inline entry omits `$ref`.
 * An unset model is simply absent — the subagent overlay's default is
 * `inherit`, and for a reference "no override" is the right reading.
 */
export function entryFromDraft(draft: RosterEntryDraft, extra: Record<string, unknown>): SubagentRosterEntry {
  const out: SubagentRosterEntry = {};
  if (draft.kind === 'reference' && draft.ref.trim()) out.$ref = draft.ref.trim();
  if (draft.description.trim()) out.description = draft.description;

  const llm: Record<string, unknown> = isPlainObject(extra['llm']) ? {...extra['llm']} : {};
  if (draft.model) llm['model'] = draft.model;
  else delete llm['model'];
  if (Object.keys(llm).length) out.llm = llm;

  if (draft.isolation) out.isolation = draft.isolation;
  if (draft.writePolicy) out.write_policy = draft.writePolicy;

  for (const [k, v] of Object.entries(extra)) {
    if (!STRUCTURED_KEYS.has(k)) out[k] = v;
  }
  return out;
}

/** The `$ref` spelling for a picked expert: library rows by their
 *  `subagents/<id>` name, bundled dirs and DB rows by id. */
export function referenceSpelling(e: Expert): string {
  if (e.source === 'library' || e.storage_kind === 'library') {
    return e.name || `subagents/${e.id}`;
  }
  return e.id;
}

/** Merge two expert lists by id, first list wins (library rows keep their
 *  `subagents/<id>` spelling over a same-id bundled row). */
export function mergeExpertsById(first: Expert[], second: Expert[]): Expert[] {
  const seen = new Set(first.map((e) => e.id));
  return [...first, ...second.filter((e) => !seen.has(e.id))];
}

/**
 * The expert's `subagents` roster: a list of entries (inline small experts or
 * `$ref`s to other experts) plus the `default` entry. Owns `default` and
 * `roster`; the roster-wide `llm` is the Model section's "Subagent model"
 * select, which the host merges in at save time. Pattern-matched to the other
 * structured groups: the host calls `prefill()` once the fragment loads and
 * `getValue()` at save, and this component keeps its own draft state in
 * between (a two-way bound value would rebuild the drafts under the author's
 * cursor on every keystroke).
 */
@Component({
  selector: 'app-subagents-editor',
  standalone: true,
  imports: [
    FormsModule,
    TranslocoPipe,
    AppButtonComponent,
    AppFormFieldComponent,
    AppIconComponent,
    AppIconButtonComponent,
    AppInputComponent,
    AppSelectComponent,
    AppSwitchComponent,
    AppTextareaComponent,
  ],
  template: `
    <div class="roster">
      @if (entries().length === 0) {
        <p class="empty">{{ 'experts.subagents.empty' | transloco }}</p>
      }

      @for (draft of entries(); track draft.key) {
        <div class="entry" [class.invalid]="issuesFor(draft.key).length > 0">
          <div class="entry-head">
            <app-form-field
              class="grow"
              [label]="'experts.subagents.name' | transloco"
              [required]="true"
              [hint]="'experts.subagents.nameHint' | transloco"
            >
              <app-input
                [value]="draft.name"
                [disabled]="disabled()"
                placeholder="explorer"
                (valueChange)="patch(draft.key, {name: $event})"
              />
            </app-form-field>
            <app-form-field [label]="'experts.subagents.kind' | transloco">
              <app-select [value]="draft.kind" [disabled]="disabled()" (valueChange)="setKind(draft.key, $event)">
                <option value="inline">{{ 'experts.subagents.kindInline' | transloco }}</option>
                <option value="reference">{{ 'experts.subagents.kindReference' | transloco }}</option>
              </app-select>
            </app-form-field>
            <app-icon-button
              class="remove"
              variant="danger"
              size="sm"
              [disabled]="disabled()"
              [ariaLabel]="'experts.subagents.remove' | transloco"
              [tooltip]="'experts.subagents.remove' | transloco"
              (clicked)="remove(draft.key)"
            >
              <app-icon size="sm">delete</app-icon>
            </app-icon-button>
          </div>

          @if (draft.kind === 'reference') {
            <app-form-field
              [label]="'experts.subagents.reference' | transloco"
              [required]="true"
              [hint]="'experts.subagents.referenceHint' | transloco"
            >
              <div class="reference-row">
                <app-select
                  class="grow"
                  [value]="draft.ref"
                  [disabled]="disabled()"
                  (valueChange)="patch(draft.key, {ref: $event ?? ''})"
                >
                  <option value="">
                    {{ (referenceLoading() ? 'experts.subagents.referenceLoading' : 'experts.subagents.referenceNone') | transloco }}
                  </option>
                  @if (draft.ref && !isKnownReference(draft.ref)) {
                    <!-- A hand-authored $ref (or one the current list does not
                         carry) still round-trips: keep it selectable. -->
                    <option [value]="draft.ref">{{ draft.ref }}</option>
                  }
                  @for (opt of referenceOptions(); track opt.value) {
                    <option [value]="opt.value">{{ opt.label }}</option>
                  }
                </app-select>
                <app-switch size="sm" [checked]="showAllExperts()" [disabled]="disabled()" (changed)="setShowAll($event)">
                  {{ 'experts.showAll' | transloco }}
                </app-switch>
              </div>
            </app-form-field>
          }

          <div class="entry-grid">
            <app-form-field
              class="span-2"
              [label]="'experts.subagents.description' | transloco"
              [hint]="'experts.subagents.descriptionHint' | transloco"
            >
              <app-textarea
                [value]="draft.description"
                [rows]="2"
                [disabled]="disabled()"
                (valueChange)="patch(draft.key, {description: $event})"
              />
            </app-form-field>
            <app-form-field [label]="'experts.subagents.model' | transloco">
              <select
                class="model-select"
                [disabled]="disabled() || modelGated()"
                [ngModel]="draft.model"
                (ngModelChange)="patch(draft.key, {model: $event})"
              >
                <option [ngValue]="''">{{ 'experts.subagents.modelInherit' | transloco }}</option>
                @for (g of models(); track g.group) {
                  <optgroup [label]="g.group">
                    @for (m of g.models; track m) {
                      <option [ngValue]="m" [disabled]="!modelAllowed()(m)">{{ modelLabel(g, m) }}</option>
                    }
                  </optgroup>
                }
              </select>
            </app-form-field>
            <app-form-field [label]="'experts.subagents.isolation' | transloco">
              <app-select [value]="draft.isolation" [disabled]="disabled()" (valueChange)="setIsolation(draft.key, $event)">
                <option value="">{{ 'experts.subagents.isolationDefault' | transloco }}</option>
                <option value="shared">{{ 'experts.subagents.isolationShared' | transloco }}</option>
                <option value="worktree">{{ 'experts.subagents.isolationWorktree' | transloco }}</option>
              </app-select>
            </app-form-field>
            <app-form-field [label]="'experts.subagents.writePolicy' | transloco">
              <app-select [value]="draft.writePolicy" [disabled]="disabled()" (valueChange)="setWritePolicy(draft.key, $event)">
                <option value="">{{ 'experts.subagents.writePolicyDefault' | transloco }}</option>
                <option value="none">{{ 'experts.subagents.writePolicyNone' | transloco }}</option>
                <option value="scratch_only">{{ 'experts.subagents.writePolicyScratchOnly' | transloco }}</option>
                <option value="owned_paths">{{ 'experts.subagents.writePolicyOwnedPaths' | transloco }}</option>
                <option value="full">{{ 'experts.subagents.writePolicyFull' | transloco }}</option>
              </app-select>
            </app-form-field>
            <app-form-field
              class="span-2"
              [label]="'experts.subagents.extra' | transloco"
              [hint]="'experts.subagents.extraHint' | transloco"
            >
              <app-textarea
                [value]="draft.extraText"
                [rows]="4"
                [disabled]="disabled()"
                placeholder='{ "tools": { "workspace": ["read_file"] }, "limits": { "max_turns": 150 }, "return": "summary" }'
                (valueChange)="patch(draft.key, {extraText: $event})"
              />
            </app-form-field>
          </div>

          @for (issue of issuesFor(draft.key); track issue) {
            <p class="issue" role="alert">{{ issue | transloco }}</p>
          }
        </div>
      }

      <div class="roster-foot">
        <app-button variant="secondary" size="sm" [disabled]="disabled()" (clicked)="add()">
          <app-icon size="sm">add</app-icon>
          {{ 'experts.subagents.add' | transloco }}
        </app-button>
        @if (entries().length > 0) {
          <app-form-field
            class="default-field"
            [label]="'experts.subagents.default' | transloco"
            [hint]="'experts.subagents.defaultHint' | transloco"
          >
            <app-select [value]="defaultEntry()" [disabled]="disabled()" (valueChange)="setDefault($event)">
              <option value="">{{ 'experts.subagents.defaultNone' | transloco }}</option>
              @for (name of names(); track name) {
                <option [value]="name">{{ name }}</option>
              }
            </app-select>
          </app-form-field>
        }
      </div>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
      }
      .roster {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
      }
      .empty {
        margin: 0;
        color: var(--text-muted);
        font-size: 0.85rem;
      }
      .entry {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
        padding: 0.75rem;
        border: 1px solid var(--border-color);
        border-left: 2px solid var(--accent-color);
        border-radius: var(--radius-control);
        background: var(--surface-0);
      }
      .entry.invalid {
        border-left-color: var(--danger);
      }
      .entry-head {
        display: flex;
        gap: 0.75rem;
        align-items: flex-start;
        flex-wrap: wrap;
      }
      .entry-head .grow {
        flex: 1 1 200px;
      }
      .entry-head .remove {
        margin-top: 1.6rem;
      }
      .reference-row {
        display: flex;
        gap: 0.75rem;
        align-items: center;
        flex-wrap: wrap;
      }
      .reference-row .grow {
        flex: 1 1 220px;
      }
      .entry-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 0.5rem 0.75rem;
      }
      .entry-grid .span-2 {
        grid-column: 1 / -1;
      }
      .model-select {
        width: 100%;
        padding: 7px 10px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        background: var(--surface-0);
        color: var(--text-primary);
        font: inherit;
      }
      .model-select:disabled {
        opacity: 0.55;
        cursor: not-allowed;
      }
      .issue {
        margin: 0;
        font-size: 0.8rem;
        color: var(--danger);
      }
      .roster-foot {
        display: flex;
        gap: 1rem;
        align-items: flex-end;
        flex-wrap: wrap;
      }
      .roster-foot .default-field {
        flex: 1 1 220px;
        max-width: 360px;
      }
    `,
  ],
})
export class SubagentsEditorComponent {
  private readonly api = inject(ApiService);

  /** The model catalog (same source as the Model row). */
  models = input<readonly ModelCatalogGroup[]>([]);
  /** Option text: the catalog label; the value stays the model id. */
  readonly modelLabel = chatModelLabel;
  /** True when a model_selection restriction is in force. */
  modelGated = input(false);
  /** Per-model allow predicate (deny-default PDP mirror). */
  modelAllowed = input<(model: string) => boolean>(() => true);
  disabled = input(false);

  change = output<void>();

  readonly entries = signal<RosterEntryDraft[]>([]);
  /** `subagents.default`; '' = none. */
  readonly defaultEntry = signal('');
  /** Keys of the stored block other than `default` / `roster` (the host owns
   *  `llm`) — carried through untouched. */
  private passthrough: Record<string, unknown> = {};
  private nextKey = 1;

  // --- reference picker -----------------------------------------------------
  readonly referenceExperts = signal<Expert[]>([]);
  readonly referenceLoading = signal(false);
  readonly showAllExperts = signal(false);
  private referencesRequested = false;
  private referenceSerial = 0;

  readonly names = computed(() =>
    this.entries()
      .map((d) => d.name.trim())
      .filter((n) => !!n),
  );

  readonly referenceOptions = computed(() =>
    this.referenceExperts().map((e) => {
      const value = referenceSpelling(e);
      const label = e.display_name && e.display_name !== value ? `${e.display_name} · ${value}` : value;
      return {value, label};
    }),
  );

  /** Per-entry validation: i18n keys, keyed by draft key. */
  readonly issues = computed(() => {
    const out = new Map<number, string[]>();
    const seen = new Set<string>();
    for (const d of this.entries()) {
      const list: string[] = [];
      const name = d.name.trim();
      if (!ROSTER_NAME_PATTERN.test(name)) {
        list.push('experts.subagents.nameInvalid');
      } else if (seen.has(name)) {
        list.push('experts.subagents.nameDuplicate');
      } else {
        seen.add(name);
      }
      if (d.kind === 'reference' && !d.ref.trim()) list.push('experts.subagents.referenceRequired');
      const extra = parseExtra(d.extraText);
      if (extra.error === 'invalid') list.push('experts.subagents.extraInvalid');
      if (extra.error === 'notObject') list.push('experts.subagents.extraNotObject');
      if (list.length) out.set(d.key, list);
    }
    return out;
  });

  readonly hasErrors = computed(() => this.issues().size > 0);

  issuesFor(key: number): string[] {
    return this.issues().get(key) ?? [];
  }

  isKnownReference(ref: string): boolean {
    return this.referenceOptions().some((o) => o.value === ref);
  }

  /** Seed the drafts from a stored `subagents` block (or nothing). */
  prefill(config: SubagentsConfig | null | undefined): void {
    const roster = isPlainObject(config?.roster) ? config!.roster! : {};
    this.entries.set(
      Object.entries(roster).map(([name, entry]) =>
        draftFromEntry(name, isPlainObject(entry) ? entry : {}, this.nextKey++),
      ),
    );
    this.defaultEntry.set(typeof config?.default === 'string' ? config.default : '');
    this.passthrough = {};
    if (isPlainObject(config)) {
      for (const [k, v] of Object.entries(config)) {
        if (k !== 'default' && k !== 'roster' && k !== 'llm') this.passthrough[k] = v;
      }
    }
    if (this.entries().some((d) => d.kind === 'reference')) this.ensureReferencesLoaded();
  }

  /**
   * The `subagents` block as the drafts describe it: `{default?, roster?}`
   * plus any passthrough keys — never `llm`, which the host adds. Entries
   * with an empty name are skipped (they cannot be keyed); `default` is
   * emitted only when it names a surviving entry. `null` when there is
   * nothing at all.
   */
  getValue(): SubagentsConfig | null {
    const roster: Record<string, SubagentRosterEntry> = {};
    for (const d of this.entries()) {
      const name = d.name.trim();
      if (!name) continue;
      roster[name] = entryFromDraft(d, parseExtra(d.extraText).value ?? {});
    }
    const out: SubagentsConfig = {...this.passthrough};
    const def = this.defaultEntry();
    if (def && def in roster) out.default = def;
    if (Object.keys(roster).length) out.roster = roster;
    return Object.keys(out).length ? out : null;
  }

  add(): void {
    this.entries.update((list) => [
      ...list,
      {
        key: this.nextKey++,
        name: '',
        kind: 'inline',
        ref: '',
        description: '',
        model: '',
        isolation: '',
        writePolicy: '',
        extraText: '',
      },
    ]);
    this.change.emit();
  }

  remove(key: number): void {
    this.entries.update((list) => list.filter((d) => d.key !== key));
    this.change.emit();
  }

  patch(key: number, changes: Partial<RosterEntryDraft>): void {
    this.entries.update((list) => list.map((d) => (d.key === key ? {...d, ...changes} : d)));
    this.change.emit();
  }

  setKind(key: number, kind: string | null): void {
    const next: RosterEntryKind = kind === 'reference' ? 'reference' : 'inline';
    this.patch(key, {kind: next});
    if (next === 'reference') this.ensureReferencesLoaded();
  }

  setIsolation(key: number, value: string | null): void {
    this.patch(key, {isolation: ISOLATIONS.includes(value as SubagentIsolation) ? (value as SubagentIsolation) : ''});
  }

  setWritePolicy(key: number, value: string | null): void {
    this.patch(key, {
      writePolicy: WRITE_POLICIES.includes(value as SubagentWritePolicy) ? (value as SubagentWritePolicy) : '',
    });
  }

  setDefault(value: string | null): void {
    this.defaultEntry.set(value ?? '');
    this.change.emit();
  }

  /** "Show all experts": any expert may be referenced — the server accepts a
   *  cross-role `$ref`; the library stays listed first under its `$ref` name. */
  setShowAll(value: boolean): void {
    this.showAllExperts.set(value);
    this.referencesRequested = true;
    this.loadReferences();
  }

  private ensureReferencesLoaded(): void {
    if (this.referencesRequested) return;
    this.referencesRequested = true;
    this.loadReferences();
  }

  private loadReferences(): void {
    const serial = ++this.referenceSerial;
    this.referenceLoading.set(true);
    const library$ = this.api.getExperts('subagent');
    const source$ = this.showAllExperts()
      ? forkJoin([library$, this.api.getExperts(undefined, {showAll: true})]).pipe(
          map(([library, all]) => mergeExpertsById(library, all)),
        )
      : library$;
    source$.subscribe((rows) => {
      if (serial !== this.referenceSerial) return;
      this.referenceExperts.set(rows);
      this.referenceLoading.set(false);
    });
  }
}
