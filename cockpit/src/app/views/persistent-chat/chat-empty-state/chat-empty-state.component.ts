import {ChangeDetectionStrategy, Component, computed, input, output} from '@angular/core';
import {RouterLink} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../../ui/icon';
import {VexillumComponent} from '../../../ui/vexillum';
import {AppSelectComponent} from '../../../ui/select';
import {AppButtonComponent} from '../../../ui/button';
import type {WorkspaceCreationView} from '../../../core/models/api.model';
import {workspaceCreationMessageKey} from '../../../core/util/workspace-creation';

/** One suggestion chip, already resolved to the active language. */
export interface DisplayedSuggestion {
  icon: string;
  text: string;
}

/**
 * Shared landing content for the chat message list when there is nothing to
 * show yet. Serves two call sites that differ only in copy and in two
 * optional elements:
 *
 * - `draft`: the pre-session composer landing ("What shall we conquer
 *   today?" before a thread exists) — shows the default-connectors control
 *   and the "Advanced options" link, neither of which mean anything until a
 *   session exists.
 * - `ready`: a connected session with no turns yet — same mark/suggestions,
 *   its own copy, no connectors control and no advanced link.
 * - `recovery`: a rejected draft creation with queued messages — keeps the
 *   connector control, adds workspace selection and retries the existing queue.
 */
@Component({
  selector: 'app-chat-empty-state',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, TranslocoPipe, AppIconComponent, VexillumComponent, AppSelectComponent, AppButtonComponent],
  styleUrl: './chat-empty-state.component.scss',
  template: `
    <div class="empty-inner" [class.draft-recovery]="variant() === 'recovery'"
         [attr.data-testid]="variant() === 'recovery' ? 'draft-recovery' : null">
      @if (variant() !== 'recovery') {
        <srw-vexillum class="empty-mark" />
      }
      <h2 class="empty-title">{{ titleKey() | transloco }}</h2>
      <p class="empty-subtitle">{{ subtitleKey() | transloco }}</p>

      @if (variant() === 'recovery') {
        <label class="draft-workspace">
          <span>{{ 'agentSettings.execution.workspaceBackend' | transloco }}</span>
          <app-select size="sm" [value]="workspaceBackend()"
                      [ariaLabel]="'agentSettings.execution.workspaceBackend' | transloco"
                      (changed)="workspaceChanged.emit($event)">
            <option value="">{{ 'chat.draft.workspaceDefault' | transloco }}</option>
            <option value="virtual">{{ 'advanced.options.virtual' | transloco }}</option>
            <option value="sandbox">{{ 'advanced.options.container' | transloco }}</option>
            <option value="vm">{{ 'advanced.options.vmQemu' | transloco }}</option>
            <option value="none">{{ 'advanced.options.none' | transloco }}</option>
          </app-select>
        </label>
      }

      @if (variant() !== 'ready') {
        <div class="draft-connectors" role="group"
             [attr.aria-label]="'chat.draft.connectorsLabel' | transloco">
          @if (connectorsLoading()) {
            <span class="draft-connectors-state">{{ 'chat.draft.connectorsLoading' | transloco }}</span>
          } @else if (connectorsError()) {
            <span class="draft-connectors-state draft-connectors-error">
              {{ 'chat.draft.connectorsFailed' | transloco }}
              <button type="button" (click)="retryRequested.emit()">
                {{ 'chat.draft.connectorsRetry' | transloco }}
              </button>
            </span>
          }
          @if ((!connectorsLoading() && !connectorsError()) || variant() === 'recovery') {
            <label class="draft-connectors-toggle">
              <input type="checkbox" [checked]="connectorsEnabled()"
                     [disabled]="connectorsLoading()"
                     (change)="onConnectorsToggle($event)">
              <span>{{ 'chat.draft.connectorsCount' | transloco: {count: datasourceCount()} }}</span>
            </label>
          }
        </div>
      }

      @if (suggestions().length > 0) {
        <div class="suggestion-grid">
          @for (s of suggestions(); track $index) {
            <button type="button" class="suggestion-chip" (click)="pick(s)">
              <app-icon size="lg" class="suggestion-icon">{{ s.icon }}</app-icon>
              <span class="suggestion-text">{{ s.text }}</span>
            </button>
          }
        </div>
      }

      @if (variant() === 'draft') {
        <a class="draft-advanced" routerLink="/sessions/new">{{ 'chat.draft.advanced' | transloco }}</a>
      } @else if (variant() === 'recovery') {
        <app-button class="draft-retry" size="sm"
                    [disabled]="connectorsLoading() || connectorsError()"
                    (clicked)="sendRetried.emit()">
          {{ 'chat.draft.retrySend' | transloco }}
        </app-button>
      }
    </div>
  `,
})
export class ChatEmptyStateComponent {
  variant = input.required<'draft' | 'ready' | 'recovery'>();
  suggestions = input.required<DisplayedSuggestion[]>();

  // Meaningless for `ready` (no connectors control renders), so optional
  // with defaults rather than required — that call site passes none of them.
  connectorsLoading = input<boolean>(false);
  connectorsError = input<boolean>(false);
  connectorsEnabled = input<boolean>(false);
  datasourceCount = input<number>(0);
  workspaceBackend = input<string>('');
  workspaceCreation = input<WorkspaceCreationView | null>(null);

  suggestionPicked = output<DisplayedSuggestion>();
  connectorsToggled = output<boolean>();
  retryRequested = output<void>();
  workspaceChanged = output<string | null>();
  sendRetried = output<void>();

  protected readonly titleKey = computed(() =>
    this.workspaceCreation() && this.variant() === 'ready' ? 'workspaceCreation.title' :
    this.variant() === 'recovery' ? 'chat.draft.recoveryTitle' :
      this.variant() === 'draft' ? 'chat.draft.title' : 'chat.empty.title',
  );
  protected readonly subtitleKey = computed(() =>
    this.workspaceCreation() && this.variant() === 'ready' ? workspaceCreationMessageKey(this.workspaceCreation()!) :
    this.variant() === 'recovery' ? 'chat.draft.recoveryBody' :
      this.variant() === 'draft' ? 'chat.draft.subtitle' : 'chat.empty.subtitle',
  );

  pick(s: DisplayedSuggestion): void {
    this.suggestionPicked.emit(s);
  }

  onConnectorsToggle(event: Event): void {
    this.connectorsToggled.emit((event.target as HTMLInputElement).checked);
  }
}
