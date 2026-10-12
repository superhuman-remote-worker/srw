import {ChangeDetectionStrategy, Component, input, model} from '@angular/core';
import {AppBadgeComponent, BadgeTone} from '../badge';
import {AppIconComponent} from '../icon';

let nextExpanderId = 0;

/**
 * A large collapsible section with a one-line summary of its contents.
 *
 * The body is hidden with the `hidden` attribute, never removed: forms put
 * stateful controls in here (signals, `@ViewChild` handles a parent reads at
 * submit time), and a collapsed section must keep them alive.
 */
@Component({
  selector: 'app-expander',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [AppBadgeComponent, AppIconComponent],
  template: `
    <h3 class="expander-heading">
      <button
        type="button"
        class="expander-trigger"
        [attr.aria-expanded]="expanded()"
        [attr.aria-controls]="bodyId"
        (click)="toggle()"
      >
        @if (icon()) {
          <span class="expander-icon" aria-hidden="true"><app-icon size="md">{{ icon() }}</app-icon></span>
        }
        <span class="expander-text">
          <span class="expander-title-row">
            <span class="expander-title">{{ heading() }}</span>
            @if (chip()) {
              <app-badge [tone]="chipTone()" shape="pill" size="sm">{{ chip() }}</app-badge>
            }
          </span>
          @if (question()) {
            <span class="expander-question">{{ question() }}</span>
          }
          @if (summary()) {
            <span class="expander-summary">{{ summary() }}</span>
          }
        </span>
        <span class="expander-chevron" aria-hidden="true"><app-icon size="md">expand_more</app-icon></span>
      </button>
    </h3>
    <div class="expander-body" [id]="bodyId" role="region" [attr.aria-label]="heading()" [hidden]="!expanded()">
      <ng-content />
    </div>
  `,
  styleUrl: './expander.component.scss',
  host: {
    '[attr.data-expanded]': 'expanded() || null',
  },
})
export class AppExpanderComponent {
  /** Section title, e.g. "Expert". */
  heading = input('');
  /** The question the section answers, shown under the title. */
  question = input('');
  /** One line describing the current selection, readable while collapsed. */
  summary = input('');
  /** Short status next to the title ("Project default", "2 changes"). */
  chip = input('');
  chipTone = input<BadgeTone>('neutral');
  /** Material icon name for the leading glyph. */
  icon = input('');
  expanded = model(false);

  protected readonly bodyId = `app-expander-body-${nextExpanderId++}`;

  protected toggle(): void {
    this.expanded.set(!this.expanded());
  }
}
