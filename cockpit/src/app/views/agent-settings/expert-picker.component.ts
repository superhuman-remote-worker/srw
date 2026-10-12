import {Component, input, output} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {Expert} from '../../core/models/api.model';
import {AppIconComponent} from '../../ui/icon';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppSwitchComponent} from '../../ui/switch';

/**
 * The Expert template grid shared by job and session creation.
 *
 * Picking a card only says which template the form starts from — the host
 * prefills the Expert section from it. Loading the list and resolving the
 * effective default stay with each page, which already owns that logic.
 */
@Component({
  selector: 'app-expert-picker',
  standalone: true,
  imports: [TranslocoPipe, AppIconComponent, AppSpinnerComponent, AppSwitchComponent],
  template: `
    <div class="picker-head">
      <span class="picker-label" [id]="labelId">{{ 'agentSettings.expertPicker.label' | transloco }}</span>
      <app-switch size="sm" [checked]="showAll()" [disabled]="disabled()" (changed)="showAllChange.emit($event)">
        {{ 'experts.showAll' | transloco }}
      </app-switch>
    </div>
    <span class="picker-hint">{{ 'agentSettings.expertPicker.hint' | transloco }}</span>
    @if (loading()) {
      <div class="picker-loading">
        <app-spinner size="sm" />
        {{ 'agentSettings.expertPicker.loading' | transloco }}
      </div>
    } @else if (experts().length > 0) {
      <div class="expert-grid" role="group" [attr.aria-labelledby]="labelId">
        @for (expert of experts(); track expert.id) {
          <button
            type="button"
            class="expert-card"
            [class.selected]="selectedId() === expert.id"
            [attr.aria-pressed]="selectedId() === expert.id"
            [disabled]="disabled()"
            (click)="picked.emit(expert)"
          >
            <span class="card-head">
              <app-icon size="md" class="expert-icon" [style.color]="expert.color || null">{{ expert.icon || 'person' }}</app-icon>
              <span class="expert-name">{{ expert.display_name }}</span>
              @if (selectedId() === expert.id) {
                <app-icon size="sm" class="expert-check">check_circle</app-icon>
              }
            </span>
            @if (expert.description) {
              <span class="expert-desc">{{ expert.description }}</span>
            }
            @if (cardTags(expert).length > 0 || expert.id === defaultId()) {
              <span class="expert-tags">
                @if (expert.id === defaultId() && defaultSource()) {
                  <span class="expert-default">{{ ('settings.expertDefaults.source.' + defaultSource()) | transloco }}</span>
                }
                @for (tag of cardTags(expert); track tag) {
                  <span class="expert-tag">{{ tag }}</span>
                }
              </span>
            }
          </button>
        }
      </div>
    } @else {
      <span class="picker-hint">{{ 'agentSettings.expertPicker.empty' | transloco }}</span>
    }
  `,
  styles: [`
    :host { display: flex; flex-direction: column; gap: 6px; }
    .picker-head {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 6px 12px;
    }
    .picker-label {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-secondary);
    }
    .picker-hint {
      font-size: 12px;
      line-height: 1.4;
      color: var(--text-muted);
    }
    .picker-loading {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 0;
      font-size: 13px;
      color: var(--text-muted);
    }
    .expert-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(min(100%, 12rem), 1fr));
      gap: 8px;
      margin-top: 4px;
    }
    .expert-card {
      display: flex;
      flex-direction: column;
      align-items: stretch;
      gap: 4px;
      min-height: 44px;
      padding: 10px 12px;
      border: 1px solid var(--border-hairline);
      border-radius: var(--radius-control);
      background: var(--surface-0);
      color: var(--text-primary);
      font: inherit;
      text-align: left;
      cursor: pointer;
      transition: border-color 0.15s ease, background-color 0.15s ease;
    }
    .expert-card:hover:not(:disabled) { border-color: var(--surface-2); }
    .expert-card:focus-visible { outline: none; box-shadow: 0 0 0 3px var(--ring); }
    .expert-card.selected {
      border-color: var(--accent-color);
      box-shadow: inset 0 0 0 1px var(--accent-color);
      background: color-mix(in srgb, var(--accent-color) 8%, var(--surface-0));
    }
    .expert-card:disabled { opacity: 0.6; cursor: not-allowed; }
    .card-head {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
    .expert-icon { flex: 0 0 auto; color: var(--text-secondary); }
    .expert-name {
      flex: 1 1 auto;
      min-width: 0;
      font-size: 14px;
      font-weight: 600;
      overflow-wrap: anywhere;
    }
    .expert-check { flex: 0 0 auto; color: var(--accent-color); }
    .expert-desc {
      font-size: 12px;
      line-height: 1.4;
      color: var(--text-secondary);
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .expert-tags { display: flex; flex-wrap: wrap; gap: 4px; }
    .expert-tag, .expert-default {
      padding: 1px 6px;
      border-radius: var(--radius-tag);
      font-size: 11px;
      background: var(--surface-1);
      color: var(--text-secondary);
    }
    .expert-default {
      background: var(--info-tint);
      color: var(--info);
      font-weight: 600;
    }
  `],
})
export class ExpertPickerComponent {
  experts = input<Expert[]>([]);
  selectedId = input<string | null>(null);
  loading = input(false);
  disabled = input(false);
  showAll = input(false);
  /** The page's own role tag, hidden on the cards: every worker expert carries
   *  `worker`, which would be noise here, while another role's tag (visible
   *  under "Show all") is what tells the two apart. */
  hideTag = input<string>('');
  /** Which expert the server resolved as the effective default, and where
   *  that default came from (`project` | `user` | `application`). */
  defaultId = input<string | null>(null);
  defaultSource = input<string | null>(null);

  picked = output<Expert>();
  showAllChange = output<boolean>();

  private static nextId = 0;
  protected readonly labelId = `expert-picker-label-${ExpertPickerComponent.nextId++}`;

  protected cardTags(expert: Expert): string[] {
    const hidden = this.hideTag();
    const tags = (expert.tags ?? []).filter((t) => t !== hidden);
    if (!tags.length && expert.expert_type && expert.expert_type !== hidden) return [expert.expert_type];
    return tags;
  }
}
