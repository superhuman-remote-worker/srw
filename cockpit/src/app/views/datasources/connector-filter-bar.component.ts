import {ChangeDetectionStrategy, Component, input, output} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppButtonComponent} from '../../ui/button';
import {AppIconComponent} from '../../ui/icon';
import {AppInputComponent} from '../../ui/input';

/** One chip of the type strip: a connector type and how many there are. */
export interface ConnectorTypeChip {
  /** The type, or 'all'. */
  value: string;
  count: number;
}

/** A removable token for one applied filter. Its label is a translation key
 *  with params that are already translated. */
export interface ConnectorFilterToken {
  id: string;
  labelKey: string;
  labelParams?: Record<string, string>;
}

/**
 * The always-visible half of the connector filters on Customize →
 * Connectors, the same shape as the jobs list's (`jobs/job-filter-bar`): a
 * strip of type chips with counts, then search, the Filters trigger and a
 * removable token for every applied filter.
 *
 * Tokens, not only a count on the Filters button: a filter you cannot see is
 * how someone concludes a connector is gone. That includes the one that is on
 * by default and hides the connectors SRW keeps for projects.
 *
 * The strip lists only the types there are connectors of (and the selected
 * one): the jobs list shows every status as a legend, but a list of thirteen
 * connector types the user mostly has none of reads as noise.
 */
@Component({
  selector: 'app-connector-filter-bar',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoPipe, AppButtonComponent, AppIconComponent, AppInputComponent],
  template: `
    <div class="cf">
      <div class="cf__chips" role="group" [attr.aria-label]="'datasources.catalog.typeGroup' | transloco">
        @for (chip of chips(); track chip.value) {
          <button
            type="button"
            class="cf-chip"
            [attr.data-type-chip]="chip.value"
            [attr.aria-pressed]="chip.value === selectedType()"
            (click)="typeSelect.emit(chip.value)"
          >
            {{ (chip.value === 'all' ? 'datasources.filter.all' : 'datasources.filter.' + chip.value) | transloco }}
            <span class="cf-chip__count">{{ chip.count }}</span>
          </button>
        }
      </div>

      <div class="cf__bar">
        @if (searchable()) {
          <app-input
            class="cf__search"
            size="sm"
            type="search"
            [fullWidth]="false"
            [value]="search()"
            [placeholder]="'datasources.catalog.search' | transloco"
            [ariaLabel]="'datasources.catalog.search' | transloco"
            (valueChange)="searchInput.emit($event)"
          />
          <app-button
            class="cf__trigger"
            size="sm"
            variant="secondary"
            data-filters-toggle
            [attr.aria-expanded]="panelOpen()"
            (clicked)="togglePanel.emit()"
          >
            <app-icon size="sm">tune</app-icon>
            {{ 'datasources.catalog.filters' | transloco }}
            @if (tokens().length) {
              <span class="cf__badge">{{ tokens().length }}</span>
            }
          </app-button>
        }

        @if (tokens().length) {
          <div class="cf__tokens" role="group" [attr.aria-label]="'datasources.catalog.appliedGroup' | transloco">
            @for (token of tokens(); track token.id) {
              <button
                type="button"
                class="cf-token"
                [attr.data-token]="token.id"
                [attr.aria-label]="'datasources.catalog.removeToken' | transloco: { filter: (token.labelKey | transloco: (token.labelParams ?? {})) }"
                (click)="removeToken.emit(token.id)"
              >
                <span aria-hidden="true">{{ token.labelKey | transloco: (token.labelParams ?? {}) }}</span>
                <span class="cf-token__x" aria-hidden="true">×</span>
              </button>
            }
            <button type="button" class="cf-token cf-token--clear" data-filters-clear (click)="clearAll.emit()">
              {{ 'datasources.catalog.clearFilters' | transloco }}
            </button>
          </div>
        }
      </div>
    </div>
  `,
  styleUrl: './connector-filter-bar.component.scss',
})
export class ConnectorFilterBarComponent {
  /** The strip: "all" first, then each type there are connectors of. */
  readonly chips = input<ConnectorTypeChip[]>([]);
  readonly selectedType = input('all');
  /** Search and the Filters panel exist only with the server-side catalog. */
  readonly searchable = input(true);
  readonly search = input('');
  readonly tokens = input<ConnectorFilterToken[]>([]);
  readonly panelOpen = input(false);

  readonly typeSelect = output<string>();
  readonly searchInput = output<string>();
  readonly togglePanel = output<void>();
  readonly removeToken = output<string>();
  readonly clearAll = output<void>();
}
