import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  input,
  output,
  signal,
  viewChild,
} from '@angular/core';
import {AppIconButtonComponent} from '../icon-button';
import {AppIconComponent} from '../icon';

/**
 * Inline, Jupyter-style rename control. Shows a piece of text and lets the user
 * edit it in place — commit on Enter or blur, cancel on Esc.
 *
 * The edit field is a chrome-less native input that inherits the surrounding
 * typography (font, size, weight, colour) and auto-sizes to its content, so the
 * title simply *becomes* editable in place rather than swapping to a smaller,
 * boxed text field.
 *
 * Presentational only: it emits `save` with the new value and never persists
 * anything itself. The parent owns the HTTP call and keeps `value` authoritative
 * (updating it optimistically and reverting on failure).
 *
 * Two entry modes:
 *  - `clickToEdit` — the text itself is clickable. Use where the title is NOT a
 *    navigation target (e.g. the chat header, a detail-page heading).
 *  - default — the text is plain and a hover pencil enters edit mode. Use inside
 *    rows/cards that navigate on click; the pencil stops the click from bubbling
 *    up so it edits instead of navigating.
 */
@Component({
  selector: 'app-inline-editable-text',
  standalone: true,
  imports: [AppIconButtonComponent, AppIconComponent],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    @if (editing()) {
      <input
        #editor
        class="inline-edit__input"
        type="text"
        [value]="draft()"
        [size]="draft().length || 1"
        [attr.aria-label]="ariaLabel() || null"
        [attr.maxlength]="maxLength()"
        [placeholder]="placeholder()"
        (input)="onInput($event)"
        (keydown)="onKeydown($event)"
        (blur)="commit()"
        (click)="$event.stopPropagation()"
      />
    } @else if (clickToEdit()) {
      <span
        class="inline-edit__text inline-edit__text--clickable"
        [title]="value()"
        (click)="startEdit(editValue() ?? value(), $event)"
        >{{ value() }}</span
      >
    } @else {
      <span class="inline-edit__display">
        <span class="inline-edit__text" [title]="value()">{{ value() }}</span>
        <app-icon-button
          class="inline-edit__pencil"
          variant="ghost"
          size="sm"
          [ariaLabel]="ariaLabel()"
          [tooltip]="ariaLabel()"
          (clicked)="startEdit(editValue() ?? value(), $event)"
        >
          <app-icon size="sm">edit</app-icon>
        </app-icon-button>
      </span>
    }
  `,
  styles: [
    `
      :host {
        display: inline-flex;
        align-items: center;
        min-width: 0;
        max-width: 100%;
      }

      .inline-edit__display {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        min-width: 0;
        max-width: 100%;
      }

      .inline-edit__text {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        min-width: 0;
      }

      .inline-edit__text--clickable {
        cursor: text;
        padding: 0 4px;
        border-radius: var(--radius-tag);
        transition: background 0.12s ease;
      }

      .inline-edit__text--clickable:hover {
        background: var(--app-hover-bg, rgba(255, 255, 255, 0.08));
      }

      /* Pencil stays out of the way until the title is hovered or focused. */
      .inline-edit__pencil {
        opacity: 0;
        flex-shrink: 0;
        transition: opacity 0.12s ease;
      }

      :host(:hover) .inline-edit__pencil,
      .inline-edit__pencil:focus-within {
        opacity: 1;
      }

      /*
       * Chrome-less in-place editor: inherit all typography from the title it
       * replaces, no box/border/background. Auto-sizes to content where
       * supported (field-sizing), with the size attribute as the fallback.
       */
      .inline-edit__input {
        font: inherit;
        color: inherit;
        letter-spacing: inherit;
        background: transparent;
        border: none;
        outline: none;
        padding: 0;
        margin: 0;
        width: auto;
        min-width: 2ch;
        max-width: 100%;
        field-sizing: content;
      }
    `,
  ],
})
export class AppInlineEditableTextComponent {
  /** The authoritative title to display. The parent owns it; we never mutate it. */
  readonly value = input.required<string>();
  /** When true the text itself is clickable; otherwise a hover pencil triggers edit. */
  readonly clickToEdit = input(false);
  /** Already-translated label for the pencil (tooltip + aria-label). */
  readonly ariaLabel = input('');
  /** Already-translated placeholder for the edit input. */
  readonly placeholder = input('');
  /**
   * What the editor starts from, when that differs from the text shown — a
   * long text shown cut off, edited from a short suggestion. Null: `value`.
   */
  readonly editValue = input<string | null>(null);
  /** Longest value the editor accepts; null for no limit. */
  readonly maxLength = input<number | null>(null);

  /** Emitted with the trimmed new value — only when it changed and is non-empty. */
  readonly save = output<string>();

  readonly editing = signal(false);
  readonly draft = signal('');

  /** The value as it was when editing began — what we diff against on commit. */
  private readonly original = signal('');
  private readonly editor = viewChild<ElementRef<HTMLInputElement>>('editor');

  startEdit(current: string, event?: Event): void {
    event?.stopPropagation();
    this.original.set(current);
    this.draft.set(current);
    this.editing.set(true);
    // Focus + select once the @if has rendered the input.
    setTimeout(() => {
      const el = this.editor()?.nativeElement;
      el?.focus();
      el?.select();
    });
  }

  onInput(event: Event): void {
    this.draft.set((event.target as HTMLInputElement).value);
  }

  commit(): void {
    // Guard the Enter → input-removed → blur sequence from committing twice.
    if (!this.editing()) return;
    this.editing.set(false);
    const next = this.draft().trim();
    if (next && next !== this.original()) {
      this.save.emit(next);
    }
  }

  cancel(): void {
    this.editing.set(false);
  }

  onKeydown(event: KeyboardEvent): void {
    if (event.key === 'Enter') {
      event.preventDefault();
      event.stopPropagation();
      this.commit();
    } else if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      this.cancel();
    }
  }
}
