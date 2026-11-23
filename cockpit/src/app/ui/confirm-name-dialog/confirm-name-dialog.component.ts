import {
  ChangeDetectionStrategy,
  Component,
  computed,
  input,
  model,
  output,
  signal,
} from '@angular/core';

import {AppButtonComponent} from '../button';
import {AppDialogComponent} from '../dialog';
import {AppInputComponent} from '../input';

/** Warning-confirm dialog with an optional type-the-name friction gate.
 *  requiredName == null → plain confirm; requiredName set → the confirm
 *  button enables only on an exact (case-sensitive) match. Built for the
 *  datasource publish flow (knowledge-base/knowledge/features/public_datasources.md) but
 *  deliberately generic (reusable for e.g. delete confirmation later). */
@Component({
  selector: 'app-confirm-name-dialog',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [AppButtonComponent, AppDialogComponent, AppInputComponent],
  template: `
    <app-dialog
      [open]="open()"
      size="sm"
      [title]="title()"
      (closed)="dismiss()"
    >
      <p class="confirm-message">{{ message() }}</p>
      @if (requiredName(); as name) {
        <p class="confirm-name-prompt">{{ namePrompt() }}</p>
        <app-input
          size="sm"
          class="mono"
          [value]="typed()"
          (valueChange)="typed.set($event)"
          [placeholder]="name"
        />
      }
      <ng-container appDialogActions>
        <app-button variant="secondary" size="sm" (clicked)="dismiss()">
          {{ cancelLabel() }}
        </app-button>
        <app-button
          [variant]="confirmVariant()"
          size="sm"
          [disabled]="!canConfirm()"
          (clicked)="confirm()"
        >
          {{ confirmLabel() }}
        </app-button>
      </ng-container>
    </app-dialog>
  `,
  styles: `
    .confirm-message {
      margin: 0 0 0.75rem;
      line-height: 1.5;
    }
    .confirm-name-prompt {
      margin: 0 0 0.5rem;
      font-size: 0.85em;
      opacity: 0.8;
    }
  `,
})
export class AppConfirmNameDialogComponent {
  readonly open = model<boolean>(false);
  readonly title = input<string>('');
  readonly message = input<string>('');
  /** Exact string the user must type; null disables the input gate. */
  readonly requiredName = input<string | null>(null);
  readonly namePrompt = input<string>('');
  readonly confirmLabel = input<string>('');
  readonly cancelLabel = input<string>('');
  /** `danger` for a confirmation that removes something for good. */
  readonly confirmVariant = input<'primary' | 'danger'>('primary');
  readonly confirmed = output<void>();
  readonly dismissed = output<void>();

  readonly typed = signal('');
  readonly canConfirm = computed(() => {
    const required = this.requiredName();
    return required === null || this.typed() === required;
  });

  confirm(): void {
    if (!this.canConfirm()) return;
    this.open.set(false);
    this.typed.set('');
    this.confirmed.emit();
  }

  dismiss(): void {
    this.open.set(false);
    this.typed.set('');
    this.dismissed.emit();
  }
}
