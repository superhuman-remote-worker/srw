import {
  AfterViewInit,
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  ElementRef,
  QueryList,
  ViewChild,
  ViewChildren,
  inject,
  input,
  output,
} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {TranslocoDirective} from '@jsverse/transloco';
import {AppInputComponent} from '../../ui/input';
import {AppButtonComponent} from '../../ui/button';
import {AppIconComponent} from '../../ui/icon';
import {JobFilterToken, JobListFilters, KNOWN_JOB_STATUSES} from './job-filters';

/**
 * The always-visible half of the jobs filter UI: status chips with
 * disjunctive counts, the search box, the Filters trigger, and the row of
 * removable applied-filter tokens.
 *
 * The tokens are not decoration. Baymard's benchmark found that showing only
 * a *count* of applied filters performs poorly — users open the panel
 * repeatedly to find out what is on. And one of ours hides rows by default
 * (archived projects), which is precisely how someone concludes a job was
 * deleted. Every active filter gets a token, including that one.
 */
@Component({
  selector: 'app-job-filter-bar',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoDirective, AppInputComponent, AppButtonComponent, AppIconComponent],
  template: `
    <div class="job-filters" *transloco="let t">
      <!-- Row 1: the status vocabulary, read as a quiet strip. A status with
           jobs is a button; an empty one is muted text, so a fresh install
           does not render as a wall of greyed-out pills. -->
      <div
        class="job-filters__chips"
        role="group"
        [attr.aria-label]="t('jobs.filter.statusGroup')"
      >
        <button
          type="button"
          class="job-chip"
          [attr.aria-pressed]="filters().status.length === 0"
          (click)="clearStatuses.emit()"
        >
          {{ t('jobs.filter.all') }}
          @if (totalCount() !== null) {
            <span class="job-chip__count">{{ totalCount() }}</span>
          }
        </button>
        @for (status of statuses; track status) {
          <button
            type="button"
            class="job-chip"
            [attr.aria-pressed]="isStatusOn(status)"
            [disabled]="isDeadEnd(status)"
            (click)="statusToggle.emit(status)"
          >
            {{ t('jobs.status.' + status) }}
            <span class="job-chip__count">{{ countFor(status) }}</span>
          </button>
        }
      </div>

      <!-- Row 2: search, the Filters trigger, and what is applied, side by
           side. The tokens sit next to the button that edits them. -->
      <div class="job-filters__bar">
        <app-input
          class="job-filters__search"
          size="sm"
          type="search"
          [fullWidth]="false"
          [value]="search()"
          [placeholder]="t('jobs.filter.searchPlaceholder')"
          [ariaLabel]="t('jobs.filter.searchLabel')"
          (valueChange)="searchInput.emit($event)"
        />
        <app-button
          #panelTrigger
          class="job-filters__trigger"
          size="sm"
          variant="secondary"
          [attr.aria-expanded]="panelOpen()"
          (clicked)="togglePanel.emit()"
        >
          <app-icon size="sm">tune</app-icon>
          {{ t('jobs.filter.more') }}
          @if (tokens().length) {
            <span class="job-filters__badge">{{ tokens().length }}</span>
          }
        </app-button>

        @if (tokens().length) {
          <div
            class="job-filters__tokens"
            role="group"
            [attr.aria-label]="t('jobs.filter.appliedGroup')"
          >
            @for (token of tokens(); track token.id) {
              <button
                #tokenButton
                type="button"
                class="job-token"
                [attr.aria-label]="
                  t('jobs.filter.removeToken', {
                    filter: t(token.labelKey, token.labelParams ?? {}),
                  })
                "
                (click)="onRemove(token, $index)"
              >
                <span aria-hidden="true">{{ t(token.labelKey, token.labelParams ?? {}) }}</span>
                <span class="job-token__x" aria-hidden="true">×</span>
              </button>
            }
            <button type="button" class="job-token job-token--clear" (click)="clearAll.emit()">
              {{ t('jobs.filter.clearAll') }}
            </button>
          </div>
        }
      </div>
    </div>
  `,
  styleUrl: './job-filter-bar.component.scss',
})
export class JobFilterBarComponent implements AfterViewInit {
  readonly filters = input.required<JobListFilters>();
  readonly tokens = input.required<JobFilterToken[]>();
  /** Disjunctive counts from /api/stats/jobs — never narrowed by the status selection. */
  readonly statusCounts = input<Record<string, number>>({});
  readonly totalCount = input<number | null>(null);
  /**
   * Held by the host so the debounce lives in one place.
   *
   * Bound to `app-input`'s `value` model rather than its `changed` output:
   * `changed` only fires on the native `change` event, i.e. on blur, which
   * for a search-as-you-type box means nothing happens until the user clicks
   * away. `valueChange` fires per keystroke, which is what the debounce is
   * there to absorb.
   */
  readonly search = input<string>('');
  readonly panelOpen = input(false);

  readonly statusToggle = output<string>();
  readonly clearStatuses = output<void>();
  readonly searchInput = output<string>();
  readonly togglePanel = output<void>();
  readonly removeToken = output<JobFilterToken>();
  readonly clearAll = output<void>();

  protected readonly statuses = KNOWN_JOB_STATUSES;

  // Decorator queries, not the signal forms: signal viewChild/viewChildren
  // never resolve under the JIT compiler the specs run on (NG0951), so the
  // focus restoration below would be untestable and silently dead there.
  // Decorator queries resolve under both JIT and AOT.
  @ViewChildren('tokenButton')
  private tokenButtons!: QueryList<ElementRef<HTMLButtonElement>>;
  @ViewChild('panelTrigger', {read: ElementRef})
  private panelTrigger?: ElementRef<HTMLElement>;

  private readonly destroyRef = inject(DestroyRef);

  /** Index whose focus we owe once the token row re-renders without it. */
  private pendingFocusIndex: number | null = null;

  ngAfterViewInit(): void {
    this.tokenButtons.changes
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => this.restoreFocus());
  }

  /**
   * Removing a token destroys the element that had focus. Without this the
   * user is dumped at <body> and has to tab back in from the top of the page.
   */
  private restoreFocus(): void {
    const index = this.pendingFocusIndex;
    if (index === null) return;
    this.pendingFocusIndex = null;
    const buttons = this.tokenButtons.toArray();
    const next = buttons[Math.min(index, buttons.length - 1)];
    (next?.nativeElement ?? this.panelTrigger?.nativeElement)?.focus();
  }

  /** Absent means zero: a chip that should read (0) must still be there. */
  protected countFor(status: string): number {
    return this.statusCounts()[status] ?? 0;
  }

  /**
   * A status with no jobs is a dead end — clicking it empties the table for
   * no reason. Disabled unless it is the one currently selected, which must
   * stay clickable so the user can get back out of it.
   */
  protected isDeadEnd(status: string): boolean {
    return this.countFor(status) === 0 && !this.isStatusOn(status);
  }

  protected isStatusOn(status: string): boolean {
    return this.filters().status.includes(status);
  }

  protected onRemove(token: JobFilterToken, index: number): void {
    this.pendingFocusIndex = index;
    this.removeToken.emit(token);
  }
}
