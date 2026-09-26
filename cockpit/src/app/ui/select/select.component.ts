import {
  AfterViewInit,
  CUSTOM_ELEMENTS_SCHEMA,
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  effect,
  inject,
  input,
  model,
  output,
  viewChild,
} from '@angular/core';
import { FocusMonitor } from '@angular/cdk/a11y';

export type SelectSize = 'sm' | 'md' | 'lg';

/**
 * Re-apply the model value whenever the projected options change.
 *
 * Options that render after the value (an async-loaded list) leave the native
 * select on its first option while the model holds another (or none), and the
 * customizable select's <selectedcontent> keeps the clone taken before Angular
 * filled in the option's label — a blank trigger. Assigning `value`, even the
 * current one, re-syncs both, so the DOM ends up where it would have been had
 * the options rendered first: the model's option, or no selection when none
 * matches. Mutations inside the trigger are ignored: the browser rewrites
 * <selectedcontent> on every assignment, and reacting to that would loop.
 */
export function syncOnOptionChange(
  el: HTMLSelectElement,
  value: () => unknown,
): MutationObserver {
  const trigger = el.querySelector('.app-select__trigger');
  const observer = new MutationObserver((records) => {
    if (records.every((r) => trigger?.contains(r.target))) return;
    const next = value();
    el.value = next == null ? '' : String(next);
  });
  observer.observe(el, {childList: true, subtree: true, characterData: true});
  return observer;
}

@Component({
  selector: 'app-select',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <select
      #selectEl
      class="app-select__field"
      [disabled]="disabled()"
      [required]="required()"
      [attr.aria-label]="ariaLabel() || null"
      [attr.aria-invalid]="invalid() || null"
      [attr.data-size]="size()"
      [attr.data-invalid]="invalid() || null"
      (change)="onChange($event)"
      (blur)="blurred.emit($event)"
      (focus)="focused.emit($event)"
    >
      <!-- Trigger for the customizable <select> (appearance: base-select, Chromium 135+).
           Ignored by browsers that don't support it, which fall back to the native popup. -->
      <button type="button" class="app-select__trigger" tabindex="-1">
        <selectedcontent></selectedcontent>
      </button>
      <ng-content></ng-content>
    </select>
    <span class="app-select__chevron" aria-hidden="true">▾</span>
  `,
  styleUrl: './select.component.scss',
  host: {
    '[attr.data-full-width]': 'fullWidth() || null',
  },
  // <selectedcontent> is a native customizable-<select> element, unknown to Angular.
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class AppSelectComponent<T = string> implements AfterViewInit {
  value = model<T | null>(null);

  size = input<SelectSize>('md');
  disabled = input<boolean>(false);
  required = input<boolean>(false);
  invalid = input<boolean>(false);
  fullWidth = input<boolean>(true);
  ariaLabel = input<string>('');

  changed = output<T | null>();
  focused = output<FocusEvent>();
  blurred = output<FocusEvent>();

  private selectEl = viewChild.required<ElementRef<HTMLSelectElement>>('selectEl');
  private focusMonitor = inject(FocusMonitor);
  private host = inject(ElementRef<HTMLElement>);
  private optionsObserver: MutationObserver | null = null;

  constructor() {
    this.focusMonitor.monitor(this.host.nativeElement, true);
    // Sync model value → DOM. Native <select> needs its `value` set imperatively
    // because the options live in projected content.
    effect(() => {
      const el = this.selectEl?.()?.nativeElement;
      if (!el) return;
      const next = this.value();
      const str = next == null ? '' : String(next);
      if (el.value !== str) el.value = str;
    });
  }

  ngAfterViewInit() {
    // Ensure value is applied once projected options are present.
    const el = this.selectEl().nativeElement;
    const next = this.value();
    el.value = next == null ? '' : String(next);
    this.optionsObserver = syncOnOptionChange(el, () => this.value());
  }

  ngOnDestroy() {
    this.optionsObserver?.disconnect();
    this.focusMonitor.stopMonitoring(this.host.nativeElement);
  }

  focus() {
    this.selectEl().nativeElement.focus();
  }

  protected onChange(event: Event) {
    const next = (event.target as HTMLSelectElement).value as unknown as T;
    this.value.set(next);
    this.changed.emit(next);
  }
}
