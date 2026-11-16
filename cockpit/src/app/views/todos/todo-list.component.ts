import { Component, computed, inject, effect, OnInit } from '@angular/core';
import { TranslocoPipe, TranslocoService } from '@jsverse/transloco';
import { TodoService } from '../../core/services/todo.service';
import { DataService } from '../../core/services/data.service';
import { isAgentModeJob } from '../../core/util/job-mode';
import { TodoItem } from '../../core/models/todo.model';
import { AppButtonComponent } from '../../ui/button';
import { AppIconButtonComponent } from '../../ui/icon-button';
import { AppBadgeComponent } from '../../ui/badge';
import { AppSelectComponent } from '../../ui/select';
import { AppSpinnerComponent } from '../../ui/spinner';

/**
 * Todo list component that displays current and archived todos.
 * Shows todos from workspace files for the selected job.
 */
@Component({
  selector: 'app-todo-list',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppIconButtonComponent,
    AppBadgeComponent,
    AppSelectComponent,
    AppSpinnerComponent,
  ],
  template: `
    <div class="todo-container">
      <!-- Header with phase selector -->
      <div class="header">
        <app-select
          size="sm"
          class="phase-selector"
          [value]="todo.selectedArchive() || 'current'"
          (changed)="onPhaseChange($event)"
        >
          <option value="current" [disabled]="!todo.currentTodos()">
            {{ 'todoList.phaseSelector.current' | transloco }}
            @if (todo.currentTodos()) {
              {{ 'todoList.phaseSelector.currentCount' | transloco: {n: todo.currentTodos()!.todos.length} }}
            }
          </option>
          @if (todo.archives().length > 0) {
            <optgroup [label]="'todoList.phaseSelector.archivedPhases' | transloco">
              @for (archive of todo.archives(); track archive.filename) {
                <option [value]="archive.filename">
                  {{ archive.phase_name || ('todoList.phaseSelector.unnamedPhase' | transloco) }}
                  @if (archive.timestamp) {
                    - {{ formatArchiveDate(archive.timestamp) }}
                  }
                </option>
              }
            </optgroup>
          }
        </app-select>
        <app-icon-button
          variant="ghost"
          size="sm"
          [ariaLabel]="'todoList.refreshTitle' | transloco"
          [tooltip]="'todoList.refreshTitle' | transloco"
          [disabled]="todo.isLoading()"
          (clicked)="todo.refresh()"
        >
          ↻
        </app-icon-button>
      </div>

      <!-- Progress bar -->
      @if (todo.displayTodos().length > 0) {
        <div class="progress-section">
          <div class="progress-bar">
            <div
              class="progress-fill"
              [style.width.%]="todo.progress().percentage"
            ></div>
          </div>
          <span class="progress-text">
            {{ todo.progress().completed }}/{{ todo.progress().total }}
            ({{ todo.progress().percentage }}%)
          </span>
        </div>
      }

      <!-- Failure note (for failed archives) -->
      @if (todo.failureNote()) {
        <div class="failure-note">
          <span class="failure-icon">&#x26A0;</span>
          <span class="failure-text">{{ todo.failureNote() }}</span>
        </div>
      }

      <!-- Loading state -->
      @if (todo.isLoading()) {
        <div class="loading-overlay">
          <app-spinner size="lg" tone="accent" />
        </div>
      }

      <!-- Error state -->
      @if (todo.error()) {
        <div class="error-state">
          <span>{{ todo.error() }}</span>
          <app-button variant="danger" size="sm" (clicked)="todo.refresh()">
            {{ 'todoList.retry' | transloco }}
          </app-button>
        </div>
      }

      <!-- No workspace -->
      @if (!todo.hasWorkspace() && !todo.isLoading() && data.currentJobId()) {
        <div class="empty-state">
          <span class="empty-icon">&#x1F4C1;</span>
          <span>{{ 'todoList.empty.noWorkspace' | transloco }}</span>
          <span class="empty-hint">{{ 'todoList.empty.noWorkspaceHint' | transloco }}</span>
        </div>
      }

      <!-- No job selected -->
      @if (!data.currentJobId() && !todo.isLoading()) {
        <div class="empty-state">
          <span class="empty-icon">&#x1F50D;</span>
          <span>{{ 'todoList.empty.noJob' | transloco }}</span>
          <span class="empty-hint">{{ 'todoList.empty.noJobHint' | transloco }}</span>
        </div>
      }

      <!-- Empty todos -->
      @if (
        todo.hasWorkspace() &&
        !todo.isLoading() &&
        !todo.error() &&
        todo.displayTodos().length === 0 &&
        data.currentJobId()
      ) {
        <div class="empty-state">
          <span class="empty-icon">&#x2705;</span>
          <span>{{ 'todoList.empty.noTodos' | transloco }}</span>
          <span class="empty-hint" data-testid="todo-empty-hint">
            {{ emptyHintKey() | transloco }}
          </span>
        </div>
      }

      <!-- Todo list -->
      @if (todo.displayTodos().length > 0) {
        <div class="todo-list">
          @for (item of todo.displayTodos(); track item.content; let i = $index) {
            <div
              class="todo-item"
              [class.completed]="item.status === 'completed'"
              [class.in-progress]="item.status === 'in_progress'"
              [class.pending]="item.status === 'pending'"
              [class.high-priority]="item.priority === 'high'"
            >
              <span class="todo-status">
                @switch (item.status) {
                  @case ('completed') { &#x2705; }
                  @case ('in_progress') { &#x1F504; }
                  @case ('pending') { &#x23F3; }
                }
              </span>
              <div class="todo-content">
                <span class="todo-text">{{ item.content }}</span>
                @if (item.notes && item.notes.length > 0) {
                  <div class="todo-notes">
                    @for (note of item.notes; track note) {
                      <span class="note">{{ note }}</span>
                    }
                  </div>
                }
              </div>
              @if (item.priority === 'high') {
                <app-badge tone="danger" appearance="solid" size="xs" [uppercase]="true">
                  {{ 'todoList.priorityHigh' | transloco }}
                </app-badge>
              }
            </div>
          }
        </div>
      }

      <!-- Summary (for archives) -->
      @if (todo.selectedArchiveData()?.summary; as summary) {
        <div class="summary-bar">
          <span class="summary-item">
            {{ 'todoList.summary.total' | transloco: {n: summary.total || todo.displayTodos().length} }}
          </span>
          <span class="summary-item completed">
            {{ 'todoList.summary.completed' | transloco: {n: summary.completed || 0} }}
          </span>
          <span class="summary-item pending">
            {{ 'todoList.summary.remaining' | transloco: {n: summary.not_completed || 0} }}
          </span>
        </div>
      }
    </div>
  `,
  styles: [`
    :host {
      display: block;
      height: 100%;
      overflow: hidden;
    }

    .todo-container {
      display: flex;
      flex-direction: column;
      height: 100%;
      background: var(--panel-bg, var(--panel-bg));
      position: relative;
    }

    /* Header */
    .header {
      display: flex;
      gap: 8px;
      padding: 8px;
      background: var(--surface-0, var(--surface-0));
      border-bottom: 1px solid var(--border-color, var(--surface-0));
      flex-shrink: 0;
    }

    app-select.phase-selector {
      flex: 1;
    }

    /* Progress section */
    .progress-section {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 8px 12px;
      background: var(--panel-header-bg);
      border-bottom: 1px solid var(--border-color, var(--surface-0));
      flex-shrink: 0;
    }

    .progress-bar {
      flex: 1;
      height: 6px;
      background: var(--surface-0, var(--surface-0));
      border-radius: var(--radius-pill);
      overflow: hidden;
    }

    .progress-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--success), var(--info));
      border-radius: var(--radius-pill);
      transition: width 0.3s ease;
    }

    .progress-text {
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      color: var(--text-muted, var(--text-muted));
      min-width: 80px;
      text-align: right;
    }

    /* Failure note */
    .failure-note {
      display: flex;
      align-items: flex-start;
      gap: 8px;
      padding: 8px 12px;
      background: var(--danger-tint);
      border-bottom: 1px solid var(--danger-tint);
      flex-shrink: 0;
    }

    .failure-icon {
      color: var(--danger);
      font-size: 14px;
    }

    .failure-text {
      font-size: 12px;
      color: var(--danger);
      line-height: 1.4;
    }

    /* Loading overlay */
    .loading-overlay {
      position: absolute;
      top: 50px;
      left: 0;
      right: 0;
      bottom: 0;
      background: rgba(17, 17, 27, 0.8);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 10;
    }

    /* Error state */
    .error-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 12px;
      padding: 40px;
      color: var(--danger);
      flex: 1;
    }

    /* Empty state */
    .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 12px;
      padding: 40px;
      color: var(--text-muted, var(--text-muted));
      flex: 1;
    }

    .empty-icon {
      font-size: 48px;
      opacity: 0.5;
    }

    .empty-hint {
      font-size: 11px;
      font-family: 'JetBrains Mono', monospace;
      opacity: 0.6;
    }

    /* Todo list */
    .todo-list {
      flex: 1;
      overflow-y: auto;
      padding: 8px;
    }

    .todo-item {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      padding: 10px 12px;
      margin-bottom: 4px;
      border-radius: var(--radius-tag);
      background: var(--surface-0, var(--surface-0));
      border-left: 3px solid var(--border-color, var(--surface-1));
      transition: all 0.15s ease;
    }

    .todo-item:hover {
      background: var(--panel-header-bg);
    }

    .todo-item.completed {
      border-left-color: var(--success);
      opacity: 0.7;
    }

    .todo-item.in-progress {
      border-left-color: var(--warning);
      background: var(--warning-tint);
    }

    .todo-item.pending {
      border-left-color: var(--info);
    }

    .todo-item.high-priority {
      border-left-color: var(--danger);
    }

    .todo-item.high-priority.pending {
      background: var(--danger-tint);
    }

    .todo-status {
      font-size: 14px;
      flex-shrink: 0;
      width: 20px;
      text-align: center;
    }

    .todo-content {
      flex: 1;
      min-width: 0;
    }

    .todo-text {
      font-size: 12px;
      color: var(--text-primary, var(--text-primary));
      line-height: 1.4;
      word-wrap: break-word;
    }

    .todo-item.completed .todo-text {
      text-decoration: line-through;
      color: var(--text-muted, var(--text-muted));
    }

    .todo-notes {
      display: flex;
      flex-direction: column;
      gap: 4px;
      margin-top: 6px;
      padding-left: 12px;
      border-left: 2px solid var(--border-color, var(--surface-1));
    }

    .note {
      font-size: 11px;
      color: var(--text-secondary, var(--text-secondary));
      font-family: 'JetBrains Mono', monospace;
    }

    /* Summary bar */
    .summary-bar {
      display: flex;
      gap: 16px;
      padding: 8px 12px;
      background: var(--surface-0, var(--surface-0));
      border-top: 1px solid var(--border-color, var(--surface-0));
      font-size: 11px;
      flex-shrink: 0;
    }

    .summary-item {
      color: var(--text-muted, var(--text-muted));
      font-family: 'JetBrains Mono', monospace;
    }

    .summary-item.completed {
      color: var(--success);
    }

    .summary-item.pending {
      color: var(--warning);
    }
  `],
})
export class TodoListComponent implements OnInit {
  readonly todo = inject(TodoService);
  readonly data = inject(DataService);
  private readonly transloco = inject(TranslocoService);

  /** The selected job runs in agent mode: it has no plan and no phases, so
   *  its current todo list stays empty for the whole run. Read from the job
   *  rows the workbench already holds; a row without the field is a worker job. */
  readonly currentJobIsAgentMode = computed(() => {
    const id = this.data.currentJobId();
    return !!id && isAgentModeJob(this.data.jobs().find((job) => job.id === id));
  });

  /** Why the list is empty: an agent-mode job (whose current list may not
   *  exist at all), a worker job that is still planning, or an archive with
   *  no todos. */
  readonly emptyHintKey = computed(() => {
    if (this.todo.selectedArchive() === null && this.currentJobIsAgentMode()) {
      return 'todoList.empty.agentModeHint';
    }
    return this.todo.isShowingCurrent() ? 'todoList.empty.planningHint' : 'todoList.empty.archiveHint';
  });

  constructor() {
    // Load todos when selected job changes
    effect(() => {
      const jobId = this.data.currentJobId();
      if (jobId) {
        this.todo.loadTodos();
      } else {
        this.todo.clear();
      }
    });
  }

  ngOnInit(): void {
    // Initial load if job already selected
    if (this.data.currentJobId()) {
      this.todo.loadTodos();
    }
  }

  onPhaseChange(value: string | null): void {
    if (!value || value === 'current') {
      this.todo.showCurrent();
    } else {
      this.todo.selectArchive(value);
    }
  }

  formatArchiveDate(timestamp: string): string {
    try {
      const date = new Date(timestamp);
      return date.toLocaleDateString(this.transloco.getActiveLang(), {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      });
    } catch {
      return timestamp;
    }
  }
}
