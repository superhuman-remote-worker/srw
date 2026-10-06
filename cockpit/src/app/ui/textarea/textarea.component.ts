import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  inject,
  input,
  model,
  output,
  viewChild,
} from '@angular/core';
import { FocusMonitor } from '@angular/cdk/a11y';

export type TextareaSize = 'sm' | 'md' | 'lg';

@Component({
  selector: 'app-textarea',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <textarea
      #textareaEl
      [attr.id]="inputId() || null"
      class="app-textarea__field"
      [value]="value()"
      [placeholder]="placeholder()"
      [disabled]="disabled()"
      [readonly]="readonly()"
      [required]="required()"
      [rows]="rows()"
      [attr.aria-label]="ariaLabel() || null"
      [attr.aria-invalid]="invalid() || null"
      [attr.data-size]="size()"
      [attr.data-invalid]="invalid() || null"
      [attr.data-resize]="resize()"
      (input)="onInput($event)"
      (change)="onChange($event)"
      (blur)="blurred.emit($event)"
      (focus)="focused.emit($event)"
    ></textarea>
  `,
  styleUrl: './textarea.component.scss',
  host: {
    '[attr.data-full-width]': 'fullWidth() || null',
  },
})
export class AppTextareaComponent {
  value = model<string>('');

  size = input<TextareaSize>('md');
  rows = input<number>(3);
  placeholder = input<string>('');
  disabled = input<boolean>(false);
  readonly = input<boolean>(false);
  required = input<boolean>(false);
  invalid = input<boolean>(false);
  fullWidth = input<boolean>(true);
  ariaLabel = input<string>('');
  /** `id` of the native control, so an `<app-form-field [forId]>` label can point at it. */
  inputId = input<string>('');
  resize = input<'none' | 'vertical' | 'horizontal' | 'both'>('vertical');

  changed = output<string>();
  focused = output<FocusEvent>();
  blurred = output<FocusEvent>();

  private textareaEl = viewChild.required<ElementRef<HTMLTextAreaElement>>('textareaEl');
  private focusMonitor = inject(FocusMonitor);
  private host = inject(ElementRef<HTMLElement>);

  constructor() {
    this.focusMonitor.monitor(this.host.nativeElement, true);
  }

  ngOnDestroy() {
    this.focusMonitor.stopMonitoring(this.host.nativeElement);
  }

  focus() {
    this.textareaEl().nativeElement.focus();
  }

  protected onInput(event: Event) {
    const next = (event.target as HTMLTextAreaElement).value;
    this.value.set(next);
  }

  protected onChange(event: Event) {
    const next = (event.target as HTMLTextAreaElement).value;
    this.changed.emit(next);
  }
}
