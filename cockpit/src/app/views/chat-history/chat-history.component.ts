import { Component, inject, computed, effect, signal, untracked } from '@angular/core';
import { TranslocoPipe, TranslocoService } from '@jsverse/transloco';
import { DataService } from '../../core/services/data.service';
import { ChatTraceService } from '../../core/services/chat-trace.service';
import { RequestService } from '../../workbench/services/request.service';
import { ChatEntry, ChatInput, ChatToolCall } from '../../core/models/chat.model';
import { AppButtonComponent } from '../../ui/button';
import { AppIconButtonComponent } from '../../ui/icon-button';
import { AppBadgeComponent, type BadgeTone } from '../../ui/badge';
import { AppSpinnerComponent } from '../../ui/spinner';

/** Transient-injection tool_call_id prefixes
 * (src/shared/runtime/core/injection_markers.py). Legacy chat rows stored the
 * re-injected block verbatim as human/tool inputs; these markers classify them
 * into the collapsed context strip. */
const INJECT_PREFIXES = [
  'instruction_inject_',
  'memory_inject_',
  'knowledge_inject_',
  'charter_inject_',
  'citation_feedback_inject_',
  'guidance_inject_',
] as const;

/** One collapsed context-strip item (transient injection descriptor). */
export interface ContextItem {
  kind: string;
  label?: string;
  hash?: string;
  chars?: number;
  /** Newer rows store full content only on the turn the block changed. */
  updated: boolean;
  key: string;
  input: ChatInput;
}

/** Per-turn view model: delta messages split from the injected context frame. */
export interface TurnVM {
  entry: ChatEntry;
  humans: { key: string; input: ChatInput }[];
  context: ContextItem[];
}

/** Element shape shared by inputs/response/reasoning for body display. */
interface Body {
  content?: string;
  content_preview?: string;
  truncated?: boolean;
  chars?: number;
}

/**
 * Chat History component that displays a clean sequential view of conversations.
 * Shows input -> response turns in a messenger-style layout.
 *
 * Pages arrive lean (previews only) from ChatTraceService; expanding a
 * truncated message, tool result, or context block hydrates the owning turn
 * via `/chat/entry/{id}` and swaps it into the list in place.
 *
 * The transient injection frame (todos, memory, knowledge, instruction files —
 * re-injected on every request for prompt-cache reasons) is collapsed into a
 * single context strip per turn instead of rendering as conversation, for both
 * new-style `type="context"` descriptors and legacy rows storing the raw block.
 */
@Component({
  selector: 'app-chat-history',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppIconButtonComponent,
    AppBadgeComponent,
    AppSpinnerComponent,
  ],
  template: `
    <div class="chat-container">
      <!-- Header -->
      <div class="chat-header">
        <span class="header-title">{{ 'chatHistory.title' | transloco }}</span>
        @if (data.currentJobId()) {
          <span class="entry-count">{{ entryCount() }}</span>
          <app-icon-button
            variant="ghost"
            size="sm"
            [ariaLabel]="'chatHistory.refresh' | transloco"
            [tooltip]="'chatHistory.refresh' | transloco"
            (clicked)="chat.refresh()"
          >
            ↻
          </app-icon-button>
        }
      </div>

      <!-- Loading State -->
      @if (chat.loading()) {
        <div class="loading-overlay">
          <app-spinner size="lg" tone="accent" />
        </div>
      }

      <!-- Error State -->
      @if (chat.error()) {
        <div class="error-state">
          <span>{{ chat.error() }}</span>
          <app-button variant="danger" size="sm" (clicked)="chat.refresh()">
            {{ 'chatHistory.retry' | transloco }}
          </app-button>
        </div>
      }

      <!-- Empty State -->
      @if (!chat.loading() && !chat.error() && turns().length === 0 && data.currentJobId()) {
        <div class="empty-state">
          <span class="empty-icon">&#x1F4AC;</span>
          <span>{{ 'chatHistory.empty.noHistory' | transloco }}</span>
          <span class="empty-hint">{{ 'chatHistory.empty.noHistoryHint' | transloco }}</span>
        </div>
      }

      <!-- No Job Selected -->
      @if (!data.currentJobId() && !chat.loading()) {
        <div class="empty-state">
          <span class="empty-icon">&#x1F50D;</span>
          <span>{{ 'chatHistory.empty.noJob' | transloco }}</span>
          <span class="empty-hint">{{ 'chatHistory.empty.noJobHint' | transloco }}</span>
        </div>
      }

      <!-- Chat Messages -->
      @if (turns().length > 0) {
        <div class="chat-list" (scroll)="onScroll($event)">
          @for (t of turns(); track t.entry._id; let idx = $index) {
            <div class="chat-turn">
              <!-- Turn Header -->
              <div class="turn-header">
                <span class="turn-number">#{{ idx + 1 }}</span>
                <app-badge [tone]="phaseTone(t.entry.phase)" size="xs" [uppercase]="true">
                  {{ t.entry.phase || ('chatHistory.phaseUnknown' | transloco) }}
                </app-badge>
                <span class="iteration">{{ 'chatHistory.iter' | transloco: {n: t.entry.iteration} }}</span>
                @if (t.entry.latency_ms) {
                  <span class="latency">{{ formatLatency(t.entry.latency_ms) }}</span>
                }
                <span class="timestamp">{{ formatTime(t.entry.timestamp) }}</span>
              </div>

              <!-- Human input messages (tool results are shown with tool calls below) -->
              @for (h of t.humans; track h.key) {
                <div class="message input-message">
                  <div class="message-header">
                    <span class="message-type human">&#x1F464; {{ 'chatHistory.human' | transloco }}</span>
                  </div>
                  <div class="message-content" [class.expanded]="isExpanded(h.key)">{{ bodyText(h.input, h.key) }}</div>
                  @if (canExpand(h.input)) {
                    <button class="expand-btn" (click)="toggleBody(h.key, t.entry._id, h.input)">
                      {{ expandLabel(h.input, h.key) }}
                    </button>
                  }
                </div>
              }

              <!-- Injected context frame (todos, memory, knowledge, …) -->
              @if (t.context.length > 0) {
                <details class="context-strip">
                  <summary class="context-header" [title]="'chatHistory.contextHint' | transloco">
                    <span class="context-icon">&#x29C9;</span>
                    <span>{{ 'chatHistory.context' | transloco }}</span>
                    <span class="context-kinds">
                      @for (c of t.context; track c.key) {
                        <span class="context-kind">
                          {{ c.label || c.kind }}@if (c.updated) {<span class="context-updated" [title]="'chatHistory.updated' | transloco">●</span>}
                        </span>
                      }
                    </span>
                  </summary>
                  <div class="context-body">
                    @for (c of t.context; track c.key) {
                      <div class="context-item">
                        <div class="context-item-header">
                          <span class="context-item-kind">{{ c.label || c.kind }}</span>
                          @if (c.chars != null) {
                            <span class="context-item-meta">{{ formatSize(c.chars) }}</span>
                          }
                          @if (c.hash) {
                            <span class="context-item-meta">#{{ c.hash }}</span>
                          }
                          @if (c.updated) {
                            <app-badge tone="warning" size="xs" [uppercase]="true">
                              {{ 'chatHistory.updated' | transloco }}
                            </app-badge>
                          }
                        </div>
                        <pre class="context-item-content" [class.expanded]="isExpanded(c.key)">{{ bodyText(c.input, c.key) }}</pre>
                        @if (canExpand(c.input)) {
                          <button class="expand-btn" (click)="toggleBody(c.key, t.entry._id, c.input)">
                            {{ expandLabel(c.input, c.key) }}
                          </button>
                        }
                      </div>
                    }
                  </div>
                </details>
              }

              <!-- Reasoning (if present) -->
              @if (t.entry.reasoning; as reasoning) {
                <details class="reasoning-section">
                  <summary class="reasoning-header">
                    <span class="reasoning-icon">&#x1F9E0;</span>
                    <span>{{ 'chatHistory.reasoning' | transloco }}</span>
                  </summary>
                  <div class="reasoning-content" [class.expanded]="isExpanded(t.entry._id + ':reason')">{{ bodyText(reasoning, t.entry._id + ':reason') }}</div>
                  @if (canExpand(reasoning)) {
                    <button class="expand-btn" (click)="toggleBody(t.entry._id + ':reason', t.entry._id, reasoning)">
                      {{ expandLabel(reasoning, t.entry._id + ':reason') }}
                    </button>
                  }
                </details>
              }

              <!-- Response Message (only show if there's content or tool calls) -->
              @if (t.entry.response.content_preview || t.entry.response.content || (t.entry.response.tool_calls && t.entry.response.tool_calls.length > 0)) {
                <div class="message response-message">
                  <div class="message-header">
                    <span class="message-type assistant">&#x1F916; {{ 'chatHistory.assistant' | transloco }}</span>
                    @if (t.entry.request_id) {
                      <span
                        class="request-link"
                        (click)="onRequestIdClick(t.entry.request_id)"
                        [title]="'chatHistory.viewRequest' | transloco"
                      >
                        {{ t.entry.request_id.slice(0, 8) }}...
                      </span>
                    }
                  </div>
                  @if (t.entry.response.content_preview || t.entry.response.content) {
                    <div class="message-content" [class.expanded]="isExpanded(t.entry._id + ':resp')">{{ bodyText(t.entry.response, t.entry._id + ':resp') }}</div>
                    @if (canExpand(t.entry.response)) {
                      <button class="expand-btn" (click)="toggleBody(t.entry._id + ':resp', t.entry._id, t.entry.response)">
                        {{ expandLabel(t.entry.response, t.entry._id + ':resp') }}
                      </button>
                    }
                  }

                  <!-- Tool Calls with Results -->
                  @if (t.entry.response.tool_calls && t.entry.response.tool_calls.length > 0) {
                    <div class="tool-calls-section">
                      @for (tc of t.entry.response.tool_calls; track tc.id) {
                        <details class="tool-call-item">
                          <summary class="tool-call-header">
                            <span class="tool-icon">&#x1F527;</span>
                            <span class="tool-name">{{ tc.name }}</span>
                            <span class="tool-args-preview">{{ tc.args_preview }}</span>
                          </summary>
                          @if (hasFullArgs(tc)) {
                            <div class="tool-call-args">
                              <pre class="tool-args-content" [class.expanded]="isExpanded(t.entry._id + ':args:' + tc.id)">{{ argsText(tc, t.entry._id + ':args:' + tc.id) }}</pre>
                              <button class="expand-btn" (click)="toggleArgs(t.entry._id, tc)">
                                {{ argsExpandLabel(tc, t.entry._id + ':args:' + tc.id) }}
                              </button>
                            </div>
                          }
                          <div class="tool-call-result" [class.expanded]="isExpanded('res:' + tc.id)">
                            @if (toolResultIndex().get(tc.id); as res) {
                              {{ bodyText(res.input, 'res:' + tc.id) }}
                            } @else if (toolResultState(t.entry._id) === 'unloaded') {
                              @if (chat.loadingMore()) {
                                <span class="no-result">
                                  <app-spinner size="sm" tone="accent" />
                                  {{ 'chatHistory.resultLoading' | transloco }}
                                </span>
                              } @else {
                                <span class="no-result">
                                  {{ 'chatHistory.resultInLaterTurn' | transloco }}
                                  <button class="expand-btn" (click)="chat.loadMore()">
                                    {{ 'chatHistory.resultLoadNext' | transloco }}
                                  </button>
                                </span>
                              }
                            } @else {
                              <span class="no-result">{{ 'chatHistory.resultNotRecorded' | transloco }}</span>
                            }
                          </div>
                          @if (toolResultIndex().get(tc.id); as res) {
                            @if (canExpand(res.input)) {
                              <button class="expand-btn" (click)="toggleBody('res:' + tc.id, res.entryId, res.input)">
                                {{ expandLabel(res.input, 'res:' + tc.id) }}
                              </button>
                            }
                          }
                        </details>
                      }
                    </div>
                  }
                </div>
              }
            </div>
          }
        </div>
      }

      <!-- Position indicator + load more -->
      @if (turns().length > 0) {
        <div class="position-bar">
          <span class="position-info">
            {{ 'chatHistory.showingTurns' | transloco: {n: turns().length} }}
            @if (chat.hasMore()) {<span class="of-total">/ {{ chat.total() }}</span>}
          </span>
          @if (chat.loadingMore()) {
            <app-spinner size="sm" tone="accent" />
          } @else if (chat.hasMore()) {
            <app-button variant="ghost" size="sm" (clicked)="chat.loadMore()">Load more</app-button>
          }
        </div>
      }
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow: hidden;
      }

      .chat-container {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: var(--panel-bg, var(--panel-bg));
        position: relative;
      }

      /* Header */
      .chat-header {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        background: var(--panel-header-bg);
        border-bottom: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
      }

      .header-title {
        font-size: 12px;
        font-weight: 600;
        color: var(--text-primary, var(--text-primary));
      }

      .entry-count {
        margin-left: auto;
        font-size: 11px;
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted, var(--text-muted));
      }

      /* Loading Overlay */
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

      /* Error State */
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

      /* Empty State */
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
        margin-top: 8px;
      }

      /* Chat List */
      .chat-list {
        flex: 1;
        overflow: auto;
        padding: 12px;
      }

      .chat-turn {
        margin-bottom: 16px;
        padding-bottom: 16px;
        border-bottom: 1px solid var(--border-color, var(--surface-0));
      }

      .chat-turn:last-child {
        border-bottom: none;
        margin-bottom: 0;
      }

      /* Turn Header */
      .turn-header {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 8px;
        font-size: 11px;
      }

      .turn-number {
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted, var(--text-muted));
      }

      .iteration {
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted, var(--text-muted));
      }

      .latency {
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted, var(--text-muted));
      }

      .timestamp {
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted, var(--text-muted));
        margin-left: auto;
      }

      /* Messages */
      .message {
        margin-bottom: 8px;
        border-radius: var(--radius-surface);
        overflow: hidden;
      }

      .input-message {
        background: var(--info-tint);
        border-left: 3px solid var(--info);
        margin-right: 40px;
      }

      .response-message {
        background: var(--success-tint);
        border-left: 3px solid var(--success);
        margin-left: 40px;
      }

      .message-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 6px 10px;
        background: rgba(0, 0, 0, 0.15);
        font-size: 11px;
      }

      .message-type {
        font-weight: 600;
      }

      .message-type.human {
        color: var(--info);
      }

      .message-type.tool {
        color: var(--accent-color);
      }

      .message-type.assistant {
        color: var(--success);
      }

      .message-content {
        padding: 10px;
        font-size: 12px;
        line-height: 1.5;
        color: var(--text-primary, var(--text-primary));
        white-space: pre-wrap;
        word-break: break-word;
        max-height: 300px;
        overflow-y: auto;
      }

      .message-content.expanded {
        max-height: none;
      }

      /* Expand / collapse (lazy hydration) */
      .expand-btn {
        display: block;
        width: 100%;
        padding: 4px 10px;
        border: none;
        background: rgba(0, 0, 0, 0.12);
        color: var(--text-muted, var(--text-muted));
        font-size: 10px;
        font-family: 'JetBrains Mono', monospace;
        text-align: left;
        cursor: pointer;
      }

      .expand-btn:hover {
        background: rgba(0, 0, 0, 0.25);
        color: var(--text-primary, var(--text-primary));
      }

      /* Injected context strip (transient todos/memory/knowledge frame) */
      .context-strip {
        margin: 8px 40px 8px 0;
        background: color-mix(in srgb, var(--text-muted) 6%, transparent);
        border-left: 3px solid var(--text-muted, var(--text-muted));
        border-radius: var(--radius-surface);
        overflow: hidden;
        opacity: 0.85;
      }

      .context-header {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 5px 10px;
        font-size: 10px;
        font-weight: 600;
        color: var(--text-muted, var(--text-muted));
        cursor: pointer;
        user-select: none;
        background: rgba(0, 0, 0, 0.1);
      }

      .context-header:hover {
        background: rgba(0, 0, 0, 0.2);
      }

      .context-icon {
        font-size: 12px;
      }

      .context-kinds {
        display: flex;
        align-items: center;
        gap: 8px;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 400;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }

      .context-kind {
        display: inline-flex;
        align-items: center;
        gap: 2px;
      }

      .context-updated {
        color: var(--warning);
        font-size: 8px;
      }

      .context-body {
        display: flex;
        flex-direction: column;
      }

      .context-item + .context-item {
        border-top: 1px solid rgba(255, 255, 255, 0.05);
      }

      .context-item-header {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 6px 10px 0;
        font-size: 10px;
        color: var(--text-muted, var(--text-muted));
      }

      .context-item-kind {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 600;
      }

      .context-item-meta {
        font-family: 'JetBrains Mono', monospace;
        opacity: 0.7;
      }

      .context-item-content {
        margin: 0;
        padding: 6px 10px 10px;
        font-size: 11px;
        line-height: 1.5;
        font-family: inherit;
        color: var(--text-muted, var(--text-muted));
        white-space: pre-wrap;
        word-break: break-word;
        max-height: 200px;
        overflow-y: auto;
      }

      .context-item-content.expanded {
        max-height: none;
        color: var(--text-primary, var(--text-primary));
      }

      /* Tool Calls Section */
      .tool-calls-section {
        border-top: 1px solid rgba(255, 255, 255, 0.05);
      }

      .tool-call-item {
        background: color-mix(in srgb, var(--accent-color) 20%, transparent);
        border-left: 3px solid var(--accent-color);
        margin: 0;
      }

      .tool-call-item + .tool-call-item {
        border-top: 1px solid rgba(255, 255, 255, 0.05);
      }

      .tool-call-header {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 10px;
        font-size: 11px;
        color: var(--accent-color);
        cursor: pointer;
        user-select: none;
        background: rgba(0, 0, 0, 0.1);
      }

      .tool-call-header:hover {
        background: rgba(0, 0, 0, 0.2);
      }

      .tool-icon {
        font-size: 14px;
        flex-shrink: 0;
      }

      .tool-name {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 600;
        flex-shrink: 0;
      }

      .tool-args-preview {
        color: var(--text-muted, var(--text-muted));
        font-family: 'JetBrains Mono', monospace;
        font-size: 10px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        flex: 1;
        min-width: 0;
      }

      .tool-call-args {
        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        background: rgba(0, 0, 0, 0.12);
      }

      .tool-args-content {
        margin: 0;
        padding: 8px 10px;
        font-size: 11px;
        line-height: 1.5;
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted, var(--text-muted));
        white-space: pre-wrap;
        word-break: break-word;
        max-height: 200px;
        overflow-y: auto;
      }

      .tool-args-content.expanded {
        max-height: none;
        color: var(--text-primary, var(--text-primary));
      }

      .tool-call-result {
        padding: 10px;
        font-size: 12px;
        line-height: 1.5;
        color: var(--text-primary, var(--text-primary));
        white-space: pre-wrap;
        word-break: break-word;
        max-height: 300px;
        overflow-y: auto;
        background: rgba(0, 0, 0, 0.05);
      }

      .tool-call-result.expanded {
        max-height: none;
      }

      .no-result {
        color: var(--text-muted, var(--text-muted));
        font-style: italic;
        display: inline-flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
      }

      /* The inline "Load it" affordance sits on the same baseline as the
         explanatory text, so it must not inherit the italic run-in style. */
      .no-result .expand-btn {
        font-style: normal;
        margin: 0;
      }

      /* Reasoning */
      .reasoning-section {
        margin: 8px 40px 8px 0;
        background: var(--warning-tint);
        border-left: 3px solid var(--warning);
        border-radius: var(--radius-surface);
        overflow: hidden;
      }

      .reasoning-header {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 8px 10px;
        font-size: 11px;
        font-weight: 600;
        color: var(--warning);
        cursor: pointer;
        user-select: none;
        background: rgba(0, 0, 0, 0.1);
      }

      .reasoning-header:hover {
        background: rgba(0, 0, 0, 0.2);
      }

      .reasoning-icon {
        font-size: 14px;
      }

      .reasoning-content {
        padding: 10px;
        font-size: 12px;
        line-height: 1.5;
        color: var(--text-primary, var(--text-primary));
        white-space: pre-wrap;
        word-break: break-word;
        max-height: 400px;
        overflow-y: auto;
      }

      .reasoning-content.expanded {
        max-height: none;
      }

      /* Request Link */
      .request-link {
        color: var(--info);
        font-family: 'JetBrains Mono', monospace;
        cursor: pointer;
        text-decoration: underline;
        text-decoration-style: dotted;
        text-underline-offset: 2px;
      }

      .request-link:hover {
        color: var(--accent-hover);
        text-decoration-style: solid;
      }

      /* Position Bar */
      .position-bar {
        display: flex;
        align-items: center;
        padding: 8px 12px;
        background: var(--surface-0, var(--surface-0));
        border-top: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
      }

      .position-info {
        font-size: 12px;
        color: var(--text-muted, var(--text-muted));
      }
    `,
  ],
})
export class ChatHistoryComponent {
  readonly data = inject(DataService);
  readonly chat = inject(ChatTraceService);
  private readonly requestService = inject(RequestService);
  private readonly transloco = inject(TranslocoService);

  /** Expanded body keys (entry-scoped, see key helpers in the template). */
  private readonly expanded = signal<Set<string>>(new Set());

  /** Keys with an in-flight hydration (expand fetched the full turn). */
  private readonly hydrating = signal<Set<string>>(new Set());

  /** Per-turn view models: delta messages split from injected context. */
  readonly turns = computed(() => this.chat.rows().map(splitTurn));

  /**
   * tool_call_id -> tool result input across all loaded turns. Results arrive
   * as the next turn's inputs; indexing the whole loaded window (instead of
   * peeking at entry idx+1) survives empty-delta turns and page boundaries.
   */
  readonly toolResultIndex = computed(() => buildToolResultIndex(this.chat.rows()));

  /**
   * Entry id of the last loaded turn — the only turn whose unresolved tool
   * calls may still be waiting on data. See {@link toolResultState}.
   */
  private readonly lastLoadedEntryId = computed(() => {
    const rows = this.chat.rows();
    return rows.length > 0 ? rows[rows.length - 1]._id : null;
  });

  // Entry count display
  readonly entryCount = computed(() => {
    return this.transloco.translate('chatHistory.turnsCount', {n: this.turns().length});
  });

  constructor() {
    // Drive the chat panel off the loaded job — the same signal the workbench
    // dashboard sets on selection (DataService.currentJobId). See
    // knowledge-base/knowledge/features/debug_audit_view_refactor.md (Phase 2c / P3).
    effect(() => {
      const jobId = this.data.currentJobId();
      untracked(() => this.chat.setJob(jobId));
    });
  }

  /**
   * Why a tool call shows no result.
   *
   * A turn's tool results are stored as the *next* turn's inputs (the archiver
   * records the messages that arrived since the last AI message). So an
   * unresolved call means one of two very different things, and only one of
   * them is a loading state:
   *
   * - `unloaded` — this is the last loaded turn and the job has more, so the
   *   result lives in a turn we simply haven't fetched. Resolvable.
   * - `missing` — the following turn IS loaded (or there is no following turn),
   *   so the result was never recorded. Nothing to wait for. Jobs that ran
   *   before the archiver delta fix lost every tool result this way; see
   *   project_chat_history_injection_bloat_lazy_hydration.
   *
   * The previous code keyed this off the global `hasMore()` alone, which
   * mislabeled every never-recorded result in a partially loaded job as
   * "arrives in a later turn".
   */
  toolResultState(entryId: string): ToolResultState {
    return resolveToolResultState(entryId, this.lastLoadedEntryId(), this.chat.hasMore());
  }

  /** Near the bottom → fetch the next page (infinite scroll). */
  onScroll(event: Event): void {
    const el = event.target as HTMLElement;
    if (el.scrollHeight - el.scrollTop - el.clientHeight < 320) {
      void this.chat.loadMore();
    }
  }

  phaseTone(phase: string | null | undefined): BadgeTone {
    switch (phase) {
      case 'strategic':
        return 'accent';
      case 'tactical':
        return 'success';
      default:
        return 'neutral';
    }
  }

  formatLatency(ms: number): string {
    if (ms < 1000) {
      return `${ms}ms`;
    }
    return `${(ms / 1000).toFixed(1)}s`;
  }

  formatTime(timestamp: string): string {
    const date = new Date(timestamp);
    return date.toLocaleTimeString(this.transloco.getActiveLang(), {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    });
  }

  formatSize(chars: number): string {
    if (chars >= 1024) {
      return `${(chars / 1024).toFixed(1)} kB`;
    }
    return `${chars} B`;
  }

  onRequestIdClick(requestId: string): void {
    this.requestService.loadRequest(requestId);
  }

  // -- lazy body expansion (lean listing + on-demand hydration) -----------

  isExpanded(key: string): boolean {
    return this.expanded().has(key);
  }

  /** Body text honoring expansion; falls back to the preview until hydrated. */
  bodyText(el: Body, key: string): string {
    if (this.isExpanded(key) && el.content != null) {
      return el.content;
    }
    return el.content_preview ?? el.content ?? '';
  }

  /** Whether a body has more than its preview (locally or via hydration). */
  canExpand(el: Body): boolean {
    return (
      el.truncated === true ||
      (el.content != null && el.content_preview != null && el.content !== el.content_preview)
    );
  }

  expandLabel(el: Body, key: string): string {
    if (this.hydrating().has(key)) {
      return '…';
    }
    if (this.isExpanded(key) && el.content != null) {
      return this.transloco.translate('chatHistory.collapse');
    }
    const size = el.chars ?? el.content?.length ?? 0;
    if (size <= 0) {
      return this.transloco.translate('chatHistory.showFullNoSize');
    }
    return this.transloco.translate('chatHistory.showFull', {
      size: this.formatSize(size),
    });
  }

  /** Toggle a body; hydrates the owning turn first when the page was lean. */
  async toggleBody(key: string, ownerEntryId: string, el: Body): Promise<void> {
    if (this.isExpanded(key)) {
      this.expanded.update((s) => {
        const next = new Set(s);
        next.delete(key);
        return next;
      });
      return;
    }
    this.expanded.update((s) => new Set(s).add(key));
    if (el.truncated === true && el.content == null) {
      this.hydrating.update((s) => new Set(s).add(key));
      try {
        await this.chat.hydrateEntry(ownerEntryId);
      } finally {
        this.hydrating.update((s) => {
          const next = new Set(s);
          next.delete(key);
          return next;
        });
      }
    }
  }

  // -- tool-call arguments (args beyond the 200-char preview) -------------

  hasFullArgs(tc: ChatToolCall): boolean {
    return tc.args != null || tc.args_truncated === true;
  }

  argsText(tc: ChatToolCall, key: string): string {
    if (this.isExpanded(key) && tc.args != null) {
      return tc.args;
    }
    return tc.args_preview;
  }

  argsExpandLabel(tc: ChatToolCall, key: string): string {
    return this.expandLabel(
      { content: tc.args, content_preview: tc.args_preview, truncated: tc.args_truncated },
      key,
    );
  }

  async toggleArgs(ownerEntryId: string, tc: ChatToolCall): Promise<void> {
    const key = `${ownerEntryId}:args:${tc.id}`;
    await this.toggleBody(key, ownerEntryId, {
      content: tc.args,
      content_preview: tc.args_preview,
      truncated: tc.args_truncated,
    });
  }
}

/** Legacy rows: injected knowledge/memory/charter/guidance/instruction blocks
 * stored as tool inputs, recognizable by their synthetic tool_call_id prefix. */
export function legacyInjectKind(input: ChatInput): string | null {
  if (input.type !== 'tool' || !input.tool_call_id) {
    return null;
  }
  for (const prefix of INJECT_PREFIXES) {
    if (input.tool_call_id.startsWith(prefix)) {
      return prefix.replace(/_inject_$/, '');
    }
  }
  return null;
}

/** Legacy rows: the transient todo block stored as a human input. */
export function isLegacyTodosInput(input: ChatInput): boolean {
  const content = input.content_preview ?? input.content ?? '';
  return input.type === 'human' && content.startsWith('<active_tasks>');
}

/** Split one entry's inputs into human delta messages + context items. */
export function splitTurn(entry: ChatEntry): TurnVM {
  const humans: TurnVM['humans'] = [];
  const context: ContextItem[] = [];
  (entry.inputs ?? []).forEach((input, idx) => {
    const key = `${entry._id}:in:${idx}`;
    if (input.type === 'context') {
      context.push({
        kind: input.kind || 'context',
        label: input.label,
        hash: input.hash,
        chars: input.chars ?? input.content?.length,
        // The writer stores full content only on the turn the block changed
        // (lean pages carry it as a truncated marker instead).
        updated: input.truncated === true || input.content != null,
        key,
        input,
      });
      return;
    }
    const legacyKind = legacyInjectKind(input);
    if (legacyKind) {
      context.push({
        kind: legacyKind,
        chars: input.chars ?? input.content?.length,
        updated: false,
        key,
        input,
      });
      return;
    }
    if (isLegacyTodosInput(input)) {
      context.push({
        kind: 'todos',
        chars: input.chars ?? input.content?.length,
        updated: false,
        key,
        input,
      });
      return;
    }
    if (input.type === 'human') {
      humans.push({ key, input });
    }
    // Real tool inputs render as the results of the calling turn's tool
    // calls (via the tool-result index), not as standalone messages.
  });
  return { entry, humans, context };
}

/**
 * tool_call_id -> tool result input across all loaded turns. Results arrive
 * as the next turn's inputs; indexing the whole loaded window (instead of
 * peeking at entry idx+1) survives empty-delta turns and page boundaries.
 */
/** Why a tool call has no result — see {@link resolveToolResultState}. */
export type ToolResultState = 'unloaded' | 'missing';

/**
 * Classify an unresolved tool call: waiting on data, or never recorded.
 *
 * Only the last loaded turn can still be waiting, because results are stored
 * as the *following* turn's inputs. If the following turn is already loaded
 * (i.e. this isn't the last row) or the job has no further turns, the result
 * was never written and no amount of paging will produce it.
 */
export function resolveToolResultState(
  entryId: string,
  lastLoadedEntryId: string | null,
  hasMore: boolean,
): ToolResultState {
  return entryId === lastLoadedEntryId && hasMore ? 'unloaded' : 'missing';
}

export function buildToolResultIndex(
  entries: ChatEntry[],
): Map<string, { entryId: string; input: ChatInput }> {
  const map = new Map<string, { entryId: string; input: ChatInput }>();
  for (const entry of entries) {
    for (const input of entry.inputs ?? []) {
      if (input.type === 'tool' && input.tool_call_id && !legacyInjectKind(input)) {
        map.set(input.tool_call_id, { entryId: entry._id, input });
      }
    }
  }
  return map;
}
