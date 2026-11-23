import {ChangeDetectionStrategy, Component, input} from '@angular/core';
import {RouterLink} from '@angular/router';
import {AppIconComponent} from '../../ui/icon';
import {SidebarToggleComponent} from '../sidebar-toggle/sidebar-toggle.component';

/**
 * The top edge of a full-page editor under Customize (expert, skill,
 * workspace template): where you came from, what you are editing, and the
 * actions that end the edit.
 *
 * It stays in view while the form scrolls, so Save is one click away from any
 * field, and it is where a save error shows: a banner at the bottom of a long
 * form is off-screen for whoever pressed Save at the top.
 *
 * The host editor is the scroll box; this header is its first child. Slots:
 * `editorIcon` (before the title), `editorBadges` (after it), `editorActions`
 * (right edge) and `editorAlert` (a full-width row under the title).
 *
 * Carries the sidebar toggle: these are full-page routes without the
 * Customize tab bar, so on a phone this is the only way back to the rail.
 */
@Component({
  selector: 'app-editor-header',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, AppIconComponent, SidebarToggleComponent],
  template: `
    <header class="eh">
      <div class="eh-row">
        <app-sidebar-toggle />
        <div class="eh-main">
          <a class="eh-back" [routerLink]="backLink()">
            <app-icon size="sm">arrow_back</app-icon>
            <span>{{ backLabel() }}</span>
          </a>
          <div class="eh-title-row">
            <ng-content select="[editorIcon]" />
            <h1 class="eh-title">{{ heading() }}</h1>
            <ng-content select="[editorBadges]" />
          </div>
        </div>
        <div class="eh-actions">
          <ng-content select="[editorActions]" />
        </div>
      </div>
      <ng-content select="[editorAlert]" />
    </header>
  `,
  styles: [`
    :host {
      display: block;
      position: sticky;
      top: 0;
      z-index: 3;
      background: var(--app-bg);
      border-bottom: 1px solid var(--border-hairline);
    }

    .eh {
      max-width: var(--editor-width, 920px);
      margin: 0 auto;
      padding: 0.75rem 1.5rem;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }

    .eh-row {
      display: flex;
      align-items: flex-end;
      gap: 0.75rem;
    }

    .eh-main {
      flex: 1;
      min-width: 0;
      display: flex;
      flex-direction: column;
      gap: 0.125rem;
    }

    .eh-back {
      align-self: flex-start;
      display: inline-flex;
      align-items: center;
      gap: 0.25rem;
      color: var(--text-muted);
      font-size: 0.8125rem;
      text-decoration: none;
      border-radius: var(--radius-tag);
    }

    .eh-back:hover {
      color: var(--text-primary);
    }

    .eh-back:focus-visible {
      outline: 2px solid var(--accent-color);
      outline-offset: 2px;
    }

    .eh-title-row {
      display: flex;
      align-items: center;
      gap: 0.625rem;
      min-width: 0;
    }

    .eh-title {
      margin: 0;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .eh-actions {
      flex-shrink: 0;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    @media (max-width: 640px) {
      .eh {
        padding: 0.5rem 1rem;
      }

      /* Two rows: the way back and the actions, then the title with the
         whole width to itself. */
      .eh-row {
        display: grid;
        grid-template-columns: auto 1fr auto;
        align-items: center;
        gap: 0.375rem 0.5rem;
      }

      .eh-main {
        display: contents;
      }

      .eh-back {
        grid-column: 2;
        grid-row: 1;
        align-self: center;
        justify-self: start;
      }

      .eh-actions {
        grid-column: 3;
        grid-row: 1;
      }

      .eh-title-row {
        grid-column: 1 / -1;
        grid-row: 2;
      }
    }
  `],
})
export class EditorHeaderComponent {
  /** What is being edited: the item's name, or "New …" on create. */
  heading = input('');
  /** The list this editor belongs to, e.g. "Experts". */
  backLabel = input('');
  /** Router link of that list. */
  backLink = input<string>('/');
}
