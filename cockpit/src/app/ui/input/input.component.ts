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

export type InputType =
  | 'text'
  | 'email'
  | 'password'
  | 'number'
  | 'search'
  | 'tel'
  | 'url';
export type InputSize = 'sm' | 'md' | 'lg';

@Component({
  selector: 'app-input',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <input
      #inputEl
      [attr.id]="inputId() || null"
      class="app-input__field"
      [type]="type()"
      [value]="value()"
      [placeholder]="placeholder()"
      [disabled]="disabled()"
      [readonly]="readonly()"
      [required]="required()"
      [attr.aria-label]="ariaLabel() || null"
      [attr.aria-invalid]="invalid() || null"
      [attr.autocomplete]="autocomplete() || null"
      [attr.inputmode]="inputmode() || null"
      [attr.list]="list() || null"
      [attr.data-size]="size()"
      [attr.data-invalid]="invalid() || null"
      (input)="onInput($event)"
      (change)="onChange($event)"
      (blur)="blurred.emit($event)"
      (focus)="focused.emit($event)"
    />
  `,
  styleUrl: './input.component.scss',
  host: {
    '[attr.data-full-width]': 'fullWidth() || null',
  },
})
export class AppInputComponent {
  value = model<string>('');

  type = input<InputType>('text');
  size = input<InputSize>('md');
  placeholder = input<string>('');
  disabled = input<boolean>(false);
  readonly = input<boolean>(false);
  required = input<boolean>(false);
  invalid = input<boolean>(false);
  fullWidth = input<boolean>(true);
  ariaLabel = input<string>('');
  /** `id` of the native control, so an `<app-form-field [forId]>` label can point at it. */
  inputId = input<string>('');
  autocomplete = input<string>('');
  inputmode = input<string>('');
  /** Optional <datalist> id for combobox-style preset suggestions (free entry still allowed). */
  list = input<string>('');

  changed = output<string>();
  focused = output<FocusEvent>();
  blurred = output<FocusEvent>();

  private inputEl = viewChild.required<ElementRef<HTMLInputElement>>('inputEl');
  private focusMonitor = inject(FocusMonitor);
  private host = inject(ElementRef<HTMLElement>);

  constructor() {
    this.focusMonitor.monitor(this.host.nativeElement, true);
  }

  ngOnDestroy() {
    this.focusMonitor.stopMonitoring(this.host.nativeElement);
  }

  focus() {
    this.inputEl().nativeElement.focus();
  }

  protected onInput(event: Event) {
    const next = (event.target as HTMLInputElement).value;
    this.value.set(next);
  }

  protected onChange(event: Event) {
    const next = (event.target as HTMLInputElement).value;
    this.changed.emit(next);
  }
}
