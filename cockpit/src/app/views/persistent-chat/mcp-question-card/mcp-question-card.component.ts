import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  Output,
  computed,
  signal,
} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppButtonComponent} from '../../../ui/button';
import {AppCheckboxComponent} from '../../../ui/checkbox';
import {AppFormFieldComponent} from '../../../ui/form-field';
import {AppIconComponent} from '../../../ui/icon';
import {AppInputComponent} from '../../../ui/input';
import type {InputType} from '../../../ui/input/input.component';
import {AppSelectComponent} from '../../../ui/select';
import {
  McpQuestion,
  McpQuestionAnswer,
  McpQuestionUiState,
  QuestionField,
  QuestionFieldError,
  QuestionValues,
  collectQuestionAnswer,
  initialQuestionValues,
  questionFields,
  questionToolLabel,
  questionUrlParts,
} from '../../../core/models/mcp-question.model';

/** Which card a question gets. */
export type QuestionCardKind = 'confirm' | 'form' | 'url';

export function questionCardKind(question: McpQuestion): QuestionCardKind {
  if (question.mode === 'url') return 'url';
  return question.confirmation ? 'confirm' : 'form';
}

let nextCardId = 0;

/**
 * An MCP server's question to the user, asked while one of its tool calls
 * runs (knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md,
 * "C2 Sessions"). Three shapes:
 *
 * - a yes/no confirmation: Accept, Decline, "Always accept in this session"
 *   and a quieter Dismiss;
 * - a form: its fields from the schema, defaults prefilled, Submit,
 *   Decline and Dismiss;
 * - a link: the full address with its host highlighted (and a warning for a
 *   punycode host), opened only by "Open link", then Done, Decline, Dismiss.
 *
 * The server's message is plain text, never HTML, and the link is never
 * fetched: no anchor, no preview, only `window.open` on a click.
 *
 * The answer goes up as `answered`; the host sends it and retires the card
 * when the question settles. Decorator inputs and an EventEmitter, so the
 * vitest JIT harness can drive the card (see helm-managed-badge).
 */
@Component({
  selector: 'app-mcp-question-card',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppCheckboxComponent,
    AppFormFieldComponent,
    AppIconComponent,
    AppInputComponent,
    AppSelectComponent,
  ],
  template: `
    @if (q(); as question) {
      <div
        class="mqc"
        role="group"
        data-testid="mcp-question-card"
        [attr.data-kind]="kind()"
        [attr.aria-labelledby]="titleId"
      >
        <div class="mqc__label">{{ 'chat.question.label' | transloco }}</div>
        <div class="mqc__title" [attr.id]="titleId">
          @if (toolLabel(); as tool) {
            {{ 'chat.question.askingDuring' | transloco: {server: question.server, tool: tool} }}
          } @else {
            {{ 'chat.question.asking' | transloco: {server: question.server} }}
          }
        </div>
        @if (question.message) {
          <!-- The server's own words: interpolated text, never HTML. -->
          <p class="mqc__message" data-testid="question-message">{{ question.message }}</p>
        }

        @switch (kind()) {
          @case ('url') {
            @if (urlParts(); as parts) {
              <div class="mqc__url" data-testid="question-url">
                <span class="mqc__url-part">{{ parts.before }}</span>
                <strong class="mqc__url-host" data-testid="question-url-host">{{ parts.host }}</strong>
                <span class="mqc__url-part">{{ parts.after }}</span>
              </div>
              @if (parts.lookAlike) {
                <p class="mqc__caution" role="alert" data-testid="question-url-lookalike">
                  <app-icon size="sm" aria-hidden="true">warning</app-icon>
                  {{ 'chat.question.lookAlike' | transloco }}
                </p>
              }
              @if (!parts.openable) {
                <p class="mqc__caution" data-testid="question-url-unopenable">
                  {{ 'chat.question.unopenableLink' | transloco }}
                </p>
              }
            }
          }
          @case ('form') {
            @if (fields().length) {
              <div class="mqc__fields">
                @for (field of fields(); track field.key) {
                  @let fieldError = errors()[field.key];
                  <app-form-field
                    class="mqc__field"
                    [attr.data-field]="field.key"
                    [label]="field.kind === 'boolean' ? '' : field.label"
                    [required]="field.required && field.kind !== 'boolean'"
                    [optional]="!field.required && field.kind !== 'boolean' ? ('chat.question.optional' | transloco) : ''"
                    [hint]="field.description ?? ''"
                    [error]="fieldError ? ('chat.question.error.' + fieldError.key | transloco: (fieldError.params ?? {})) : ''"
                    [forId]="inputId(field.key)"
                  >
                    @switch (field.kind) {
                      @case ('boolean') {
                        <app-checkbox
                          size="sm"
                          [checked]="checked(field.key)"
                          [disabled]="locked()"
                          (changed)="toggle(field.key, $event)"
                        >{{ field.label }}</app-checkbox>
                      }
                      @case ('select') {
                        <app-select
                          size="sm"
                          [inputId]="inputId(field.key)"
                          [value]="text(field.key)"
                          [required]="field.required"
                          [invalid]="!!fieldError"
                          [disabled]="locked()"
                          (changed)="pick(field.key, $event)"
                        >
                          <option value="">{{ 'chat.question.choose' | transloco }}</option>
                          @for (option of field.options; track $index) {
                            <option [value]="'' + $index">{{ option.label }}</option>
                          }
                        </app-select>
                      }
                      @case ('multi') {
                        <div class="mqc__choices" role="group" [attr.aria-label]="field.label">
                          @for (option of field.options; track $index) {
                            <app-checkbox
                              size="sm"
                              [checked]="isPicked(field.key, $index)"
                              [disabled]="locked()"
                              (changed)="toggleOption(field.key, $index, $event)"
                            >{{ option.label }}</app-checkbox>
                          }
                        </div>
                      }
                      @default {
                        <app-input
                          size="sm"
                          autocomplete="off"
                          [inputId]="inputId(field.key)"
                          [type]="inputType(field)"
                          [value]="text(field.key)"
                          [required]="field.required"
                          [invalid]="!!fieldError"
                          [min]="field.minimum"
                          [max]="field.maximum"
                          [step]="field.kind === 'integer' ? 1 : field.kind === 'number' ? 'any' : null"
                          [minLength]="field.minLength"
                          [maxLength]="field.maxLength"
                          [disabled]="locked()"
                          (valueChange)="setText(field.key, $event)"
                        />
                      }
                    }
                  </app-form-field>
                }
              </div>
            }
          }
        }

        <p class="mqc__secret" data-testid="question-secret-warning">
          <app-icon size="sm" aria-hidden="true">lock</app-icon>
          {{ 'chat.question.secretWarning' | transloco }}
        </p>

        @if (ui().error; as error) {
          <p class="mqc__error" role="alert" data-testid="question-error">{{ error }}</p>
        } @else if (ui().sent) {
          <p class="mqc__sent" role="status" data-testid="question-sent">
            {{ 'chat.question.sent' | transloco: {server: question.server} }}
          </p>
        }

        <div class="mqc__actions">
          @switch (kind()) {
            @case ('confirm') {
              <app-button variant="success" size="sm" data-action="accept"
                          [disabled]="locked()" [loading]="!!ui().busy"
                          (clicked)="accept()">{{ 'chat.question.accept' | transloco }}</app-button>
              <app-button variant="info" size="sm" data-action="always-accept"
                          [disabled]="locked()"
                          [title]="'chat.question.alwaysAcceptHint' | transloco: {server: question.server}"
                          (clicked)="alwaysAccept()">{{ 'chat.question.alwaysAccept' | transloco }}</app-button>
              <app-button variant="secondary" size="sm" data-action="decline"
                          [disabled]="locked()"
                          (clicked)="decline()">{{ 'chat.question.decline' | transloco }}</app-button>
            }
            @case ('form') {
              <app-button variant="success" size="sm" data-action="submit"
                          [disabled]="locked()" [loading]="!!ui().busy"
                          (clicked)="submit()">{{ 'chat.question.submit' | transloco }}</app-button>
              <app-button variant="secondary" size="sm" data-action="decline"
                          [disabled]="locked()"
                          (clicked)="decline()">{{ 'chat.question.decline' | transloco }}</app-button>
            }
            @case ('url') {
              <app-button variant="primary" size="sm" data-action="open-link"
                          [disabled]="!urlParts()?.openable"
                          (clicked)="openLink()">
                <app-icon size="sm" aria-hidden="true">open_in_new</app-icon>
                {{ 'chat.question.openLink' | transloco }}
              </app-button>
              <app-button variant="success" size="sm" data-action="done"
                          [disabled]="locked()" [loading]="!!ui().busy"
                          [title]="'chat.question.doneHint' | transloco: {server: question.server}"
                          (clicked)="done()">{{ 'chat.question.done' | transloco }}</app-button>
              <app-button variant="secondary" size="sm" data-action="decline"
                          [disabled]="locked()"
                          (clicked)="decline()">{{ 'chat.question.decline' | transloco }}</app-button>
            }
          }
          <app-button variant="ghost" size="sm" data-action="dismiss"
                      [disabled]="locked()"
                      [title]="'chat.question.dismissHint' | transloco"
                      (clicked)="dismiss()">{{ 'chat.question.dismiss' | transloco }}</app-button>
        </div>
      </div>
    }
  `,
  styleUrl: './mcp-question-card.component.scss',
})
export class McpQuestionCardComponent {
  protected readonly q = signal<McpQuestion | null>(null);
  protected readonly ui = signal<McpQuestionUiState>({});
  protected readonly values = signal<QuestionValues>({});
  protected readonly errors = signal<Record<string, QuestionFieldError>>({});
  private readonly uid = ++nextCardId;
  protected readonly titleId = `mqc-title-${this.uid}`;

  @Input({required: true}) set question(value: McpQuestion) {
    const previous = this.q();
    this.q.set(value);
    // A re-announced question with the same request keeps what was typed.
    if (previous?.approvalId !== value.approvalId) {
      this.values.set(initialQuestionValues(this.fieldsOf(value)));
      this.errors.set({});
    }
  }

  @Input() set state(value: McpQuestionUiState | null | undefined) {
    this.ui.set(value ?? {});
  }

  @Output() readonly answered = new EventEmitter<McpQuestionAnswer>();

  protected readonly kind = computed<QuestionCardKind>(() => {
    const question = this.q();
    return question ? questionCardKind(question) : 'form';
  });
  readonly fields = computed<QuestionField[]>(() => {
    const question = this.q();
    return question ? this.fieldsOf(question) : [];
  });
  protected readonly urlParts = computed(() => {
    const question = this.q();
    return question?.mode === 'url' ? questionUrlParts(question.url) : null;
  });
  protected readonly toolLabel = computed(() => {
    const question = this.q();
    return question ? questionToolLabel(question.tool, question.serverSlug) : null;
  });
  /** Nothing changes while an answer is on its way or has landed. */
  protected readonly locked = computed(() => !!this.ui().busy || !!this.ui().sent);

  private fieldsOf(question: McpQuestion): QuestionField[] {
    return questionCardKind(question) === 'form' ? questionFields(question.schema) : [];
  }

  protected inputId(key: string): string {
    return `mqc-${this.uid}-${key}`;
  }

  protected inputType(field: QuestionField): InputType {
    switch (field.kind) {
      case 'email':
        return 'email';
      case 'url':
        return 'url';
      case 'date':
        return 'date';
      case 'datetime':
        return 'datetime-local';
      case 'number':
      case 'integer':
        return 'number';
      default:
        return 'text';
    }
  }

  protected text(key: string): string {
    const value = this.values()[key];
    return typeof value === 'string' ? value : '';
  }

  protected checked(key: string): boolean {
    return this.values()[key] === true;
  }

  protected isPicked(key: string, index: number): boolean {
    const value = this.values()[key];
    return Array.isArray(value) && value.includes(index);
  }

  setText(key: string, value: string): void {
    this.patch(key, value);
  }

  toggle(key: string, value: boolean): void {
    this.patch(key, value);
  }

  pick(key: string, value: string | null): void {
    this.patch(key, value ?? '');
  }

  toggleOption(key: string, index: number, on: boolean): void {
    const current = this.values()[key];
    const picked = Array.isArray(current) ? current.filter((i) => i !== index) : [];
    if (on) picked.push(index);
    this.patch(key, picked.sort((a, b) => a - b));
  }

  private patch(key: string, value: QuestionValues[string]): void {
    this.values.update((values) => ({...values, [key]: value}));
    if (this.errors()[key]) {
      this.errors.update((errors) => {
        const next = {...errors};
        delete next[key];
        return next;
      });
    }
  }

  accept(): void {
    const confirmation = this.q()?.confirmation;
    if (!confirmation || this.locked()) return;
    this.answered.emit({decision: 'approve', content: confirmation});
  }

  alwaysAccept(): void {
    const confirmation = this.q()?.confirmation;
    if (!confirmation || this.locked()) return;
    this.answered.emit({decision: 'approve', content: confirmation, alwaysAccept: true});
  }

  submit(): void {
    if (this.locked()) return;
    const draft = collectQuestionAnswer(this.fields(), this.values());
    this.errors.set(draft.errors);
    if (Object.keys(draft.errors).length > 0) return;
    this.answered.emit({decision: 'approve', content: draft.content});
  }

  /** URL mode: the user finished on the server's page. */
  done(): void {
    if (this.locked()) return;
    this.answered.emit({decision: 'approve'});
  }

  decline(): void {
    if (this.locked()) return;
    this.answered.emit({decision: 'deny'});
  }

  dismiss(): void {
    if (this.locked()) return;
    this.answered.emit({decision: 'cancel'});
  }

  /** Open the server's page, only now and only in a new tab that cannot
   *  reach back to this one. */
  openLink(): void {
    const parts = this.urlParts();
    if (!parts?.openable) return;
    window.open(parts.href, '_blank', 'noopener,noreferrer');
  }
}
