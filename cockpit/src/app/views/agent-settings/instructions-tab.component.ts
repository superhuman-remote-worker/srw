import {Component, computed, input, output, signal} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppButtonComponent} from '../../ui/button';

/**
 * Instructions tab: full-height markdown textarea with clear and reset actions.
 * Used only in job creation mode. Emits content changes via contentChange for
 * the parent (job-create) to mirror into the JobArtifactService form state.
 */
@Component({
  selector: 'app-instructions-tab',
  standalone: true,
  imports: [FormsModule, AppSpinnerComponent, AppButtonComponent],
  template: `
    <div class="instructions-container">
      <div class="instructions-header">
        <label class="instructions-label">Custom Instructions</label>
        @if (loadingExpert()) {
          <app-spinner size="sm" />
        }
      </div>

      <textarea
        class="instructions-editor"
        [ngModel]="content()"
        (ngModelChange)="onEdit($event)"
        [rows]="rows()"
        placeholder="Select an expert to pre-fill instructions, or type custom instructions..."
        [disabled]="disabled() || loadingExpert()"
      ></textarea>

      <div class="instructions-actions">
        @if (content()) {
          <app-button variant="ghost" size="sm" [disabled]="disabled()" (clicked)="clearContent()">Clear</app-button>
        }
        @if (hasExpertDefault()) {
          <app-button variant="ghost" size="sm" [disabled]="disabled() || loadingExpert()" (clicked)="resetToExpert()">Reset to expert default</app-button>
        }
      </div>
    </div>
  `,
  styles: [`
    .instructions-container {
      display: flex;
      flex-direction: column;
      height: 100%;
      gap: 8px;
    }
    .instructions-header {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .instructions-label {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-primary, var(--text-primary));
    }
    .instructions-editor {
      flex: 1;
      min-height: 8rem;
      padding: 12px 14px;
      border: 1px solid var(--border-color, var(--surface-1));
      border-radius: var(--radius-control);
      background: var(--surface-0, var(--surface-0));
      color: var(--text-primary, var(--text-primary));
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px;
      line-height: 1.5;
      resize: vertical;
      transition: border-color 0.15s;
    }
    .instructions-editor:focus {
      outline: none;
      border-color: var(--accent-color, var(--accent-color));
    }
    .instructions-editor:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .instructions-editor::placeholder {
      color: var(--text-muted);
    }
    .instructions-actions {
      display: flex;
      gap: 12px;
    }
  `],
})
export class InstructionsTabComponent {
  disabled = input(false);
  loadingExpert = input(false);
  /** Visible height of the editor. */
  rows = input(16);

  contentChange = output<string | null>();

  /** Current instructions content. */
  readonly content = signal<string | null>(null);
  /** Expert default instructions (set by parent when expert detail loads). */
  private expertDefault: string | null = null;

  readonly hasExpertDefault = computed(() => !!this.expertDefault);

  readonly isModified = computed(() => {
    const c = this.content();
    return c !== null && c !== this.expertDefault;
  });

  onEdit(value: string): void {
    this.content.set(value || null);
    this.contentChange.emit(value || null);
  }

  clearContent(): void {
    this.content.set(null);
    this.contentChange.emit(null);
  }

  resetToExpert(): void {
    this.content.set(this.expertDefault);
    this.contentChange.emit(this.expertDefault);
  }

  /** Called by parent to set both the content and the expert default. */
  setFromExpert(instructions: string | null): void {
    this.expertDefault = instructions;
    this.content.set(instructions);
    this.contentChange.emit(instructions);
  }

  /** Called by parent to set content from an external source (e.g. expert prefill). */
  setContent(value: string | null): void {
    this.content.set(value);
  }

  resetAll(): void {
    this.content.set(null);
    this.expertDefault = null;
  }
}
