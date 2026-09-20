import {ChangeDetectionStrategy, Component, computed, input, output} from '@angular/core';
import {TranslocoDirective} from '@jsverse/transloco';
import {AppSelectComponent} from '../../ui/select';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppIconComponent} from '../../ui/icon';

/**
 * Pagination footer for the jobs list.
 *
 * Purely presentational — the host owns the filter state and the persisted
 * page-size preference. Counts are expressed in *display roots*, matching the
 * server: a parent's children ride along with it and are never counted
 * against the page size, so "1–25 of 119" stays true even though the page may
 * render more than 25 rows.
 */
@Component({
  selector: 'app-job-list-footer',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoDirective, AppSelectComponent, AppIconButtonComponent, AppIconComponent],
  template: `
    <nav
      class="job-footer"
      *transloco="let t"
      [attr.aria-label]="t('jobs.pagination.label')"
    >
      <p class="job-footer__range" aria-live="polite">
        @if (total() !== null) {
          {{
            t('jobs.count', {
              start: range().start,
              end: range().end,
              total: totalLabel(),
            })
          }}
        } @else {
          {{ t('jobs.pagination.rangeUnknown', {start: range().start, end: range().end}) }}
        }
      </p>

      <div class="job-footer__controls">
        <label class="job-footer__size">
          <span class="job-footer__size-label">{{ t('jobs.pagination.pageSize') }}</span>
          <app-select
            size="sm"
            [fullWidth]="false"
            [value]="pageSize()"
            [ariaLabel]="t('jobs.pagination.pageSize')"
            (changed)="onPageSize($event)"
          >
            @for (option of pageSizeOptions(); track option) {
              <option [value]="option">{{ option }}</option>
            }
          </app-select>
        </label>

        <span class="job-footer__page" aria-current="page">
          {{ t('jobs.pagination.current', {page: page()}) }}
        </span>

        <app-icon-button
          size="sm"
          variant="ghost"
          [disabled]="!hasPrevious() || loading()"
          [ariaLabel]="
            hasPrevious()
              ? t('jobs.pagination.goToPage', {page: page() - 1})
              : t('jobs.pagination.previous')
          "
          [tooltip]="t('jobs.pagination.previous')"
          (clicked)="pageChange.emit(page() - 1)"
        >
          <app-icon size="sm">chevron_left</app-icon>
        </app-icon-button>
        <app-icon-button
          size="sm"
          variant="ghost"
          [disabled]="!hasNext() || loading()"
          [ariaLabel]="
            hasNext()
              ? t('jobs.pagination.goToPage', {page: page() + 1})
              : t('jobs.pagination.next')
          "
          [tooltip]="t('jobs.pagination.next')"
          (clicked)="pageChange.emit(page() + 1)"
        >
          <app-icon size="sm">chevron_right</app-icon>
        </app-icon-button>
      </div>
    </nav>
  `,
  styleUrl: './job-list-footer.component.scss',
})
export class JobListFooterComponent {
  /** 1-based. */
  readonly page = input.required<number>();
  readonly pageSize = input.required<number>();
  /** Display roots on THIS page — the last page is short. */
  readonly count = input.required<number>();
  /** Null when the caller opted out of the count (page > 1 carries it). */
  readonly total = input<number | null>(null);
  readonly totalIsCapped = input(false);
  readonly hasMore = input(false);
  readonly loading = input(false);
  readonly pageSizeOptions = input<readonly number[]>([25, 50, 100]);

  readonly pageChange = output<number>();
  readonly pageSizeChange = output<number>();

  /**
   * Derived from the rows actually returned rather than from pageSize, so a
   * short final page reports "101–119", not "101–125".
   */
  readonly range = computed(() => {
    const start = (this.page() - 1) * this.pageSize() + 1;
    const count = this.count();
    return {start: count === 0 ? 0 : start, end: count === 0 ? 0 : start + count - 1};
  });

  /** "10000+" past the server's counting cap — the count is bounded on purpose. */
  readonly totalLabel = computed(() => {
    const total = this.total();
    if (total === null) return '';
    return this.totalIsCapped() ? `${total}+` : `${total}`;
  });

  readonly hasPrevious = computed(() => this.page() > 1);
  readonly hasNext = computed(() => this.hasMore());

  protected onPageSize(value: unknown): void {
    const size = Number(value);
    if (Number.isFinite(size) && size > 0) {
      this.pageSizeChange.emit(size);
    }
  }
}
