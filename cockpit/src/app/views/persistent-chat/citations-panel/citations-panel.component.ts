import {
    ChangeDetectionStrategy,
    Component,
    DOCUMENT,
    computed,
    effect,
    inject,
    output,
    signal,
    untracked,
} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';

import {openCitationSnapshot, snapshotViewType} from '../../../core/markdown/citation-snapshot';
import {
    CitationState,
    citationReasonLabel,
    citationState,
    citationStateLabel,
} from '../../../core/markdown/citation-status';
import {
    CitationDrift,
    PersistentChatService,
    ThreadCitation,
} from '../../../core/services/persistent-chat.service';
import {AppBadgeComponent, BadgeTone} from '../../../ui/badge';
import {AppIconComponent} from '../../../ui/icon';

interface CitationRow extends ThreadCitation {
    /** 1..k in id (creation) order — a stable reference number for the list. */
    index: number;
    /** Source is reachable by URL → render the name as a link. */
    isWebLink: boolean;
    state: CitationState;
    /** i18n key of the reason a citation is not verified, or null. */
    reasonKey: string | null;
}

// Kept for the spec and callers that imported it from here.
export {snapshotViewType};

/** Per-citation drift state: undefined = not checked, 'loading' = in flight. */
type DriftState = CitationDrift | 'loading' | undefined;

/**
 * Session citations panel (Phase 4 Half-B v2). Lists the engine-backed citations
 * for the open session — the same `citationsByCid` the inline chips resolve
 * against — with their state and, when not verified, the reason; withdrawn
 * ones stay listed, struck through (D7). For cloud-document citations a
 * "view original" link (streams the cite-time snapshot) and an on-demand drift
 * check. Both reuse the by-citation Phase-3 endpoints, which now authorize
 * session citations by thread ownership.
 */
@Component({
    selector: 'app-citations-panel',
    standalone: true,
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [TranslocoPipe, AppBadgeComponent, AppIconComponent],
    template: `
    <div class="citations-panel">
      <div class="cp-header">
        <span class="cp-title">{{ 'chat.citations.title' | transloco }} ({{ rows().length }})</span>
        <button class="cp-close" type="button" (click)="close.emit()"
                [title]="'chat.citations.close' | transloco">
          <app-icon size="sm">close</app-icon>
        </button>
      </div>

      @if (rows().length === 0) {
        <div class="cp-empty">{{ 'chat.citations.empty' | transloco }}</div>
      } @else {
        <ul class="cp-list">
          @for (c of rows(); track c.id) {
            <li class="cp-item" [class.cp-item--focus]="focusedId() === c.id" [attr.data-cid]="c.id">
              <div class="cp-item-head">
                <span class="cp-index">{{ c.index }}</span>
                @if (c.isWebLink) {
                  <a class="cp-source" [href]="c.source_identifier" target="_blank" rel="noopener">{{ c.source_name }}</a>
                } @else {
                  <span class="cp-source">{{ c.source_name }}</span>
                }
                <app-badge [tone]="stateTone(c.state)" size="xs">
                  {{ stateLabel(c.state) | transloco }}
                </app-badge>
              </div>

              <div class="cp-claim" [class.cp-claim--withdrawn]="c.state === 'withdrawn'">{{ c.claim }}</div>
              @if (c.state === 'withdrawn' && c.withdrawn_reason) {
                <div class="cp-reason">{{ c.withdrawn_reason }}</div>
              } @else if (c.reasonKey || ((c.state === 'failed' || c.state === 'unchecked') && c.verification_notes)) {
                <div class="cp-reason">
                  @if (c.reasonKey) {
                    <strong>{{ c.reasonKey | transloco }}</strong>
                  }
                  @if (c.verification_notes) {
                    <span>{{ c.reasonKey ? ' — ' : '' }}{{ c.verification_notes }}</span>
                  }
                </div>
              }

              @if (c.has_cloud_anchor || c.has_snapshot) {
                <div class="cp-cloud">
                  @if (c.has_cloud_anchor) {
                    @if (driftOf(c.id); as d) {
                      @if (d === 'loading') {
                        <span class="cp-muted">{{ 'chat.citations.checking' | transloco }}</span>
                      } @else {
                        <app-badge [tone]="driftTone(d.live_state)" size="xs">
                          {{ driftLabel(d.live_state) | transloco }}
                        </app-badge>
                      }
                    } @else {
                      <button class="cp-link" type="button" (click)="checkDrift(c.id)">
                        <app-icon size="xs">sync</app-icon>
                        {{ 'chat.citations.checkSource' | transloco }}
                      </button>
                    }
                  }
                  @if (c.has_snapshot) {
                    <button class="cp-link" type="button" (click)="viewOriginal(c.id)">
                      <app-icon size="xs">description</app-icon>
                      {{ 'chat.citations.viewOriginal' | transloco }}
                    </button>
                  }
                </div>
              }
            </li>
          }
        </ul>
      }
    </div>
  `,
    styles: [
        `
    .citations-panel {
      display: flex;
      flex-direction: column;
      max-height: 60vh;
      overflow: hidden;
    }
    .cp-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.5rem 0.75rem;
      border-bottom: 1px solid var(--border-color, rgba(127, 127, 127, 0.2));
    }
    .cp-title { font-weight: 600; font-size: 0.9rem; }
    .cp-close {
      background: none; border: none; cursor: pointer;
      color: inherit; display: inline-flex; padding: 0.15rem;
    }
    .cp-empty { padding: 1rem 0.75rem; opacity: 0.7; font-size: 0.85rem; }
    .cp-list { list-style: none; margin: 0; padding: 0.25rem; overflow-y: auto; }
    .cp-item {
      padding: 0.5rem 0.5rem;
      border-bottom: 1px solid var(--border-color, rgba(127, 127, 127, 0.12));
    }
    .cp-item:last-child { border-bottom: none; }
    .cp-item-head { display: flex; align-items: center; gap: 0.4rem; }
    .cp-index {
      flex: 0 0 auto; min-width: 1.4rem; height: 1.4rem;
      display: inline-flex; align-items: center; justify-content: center;
      border-radius: 50%; font-size: 0.72rem; font-weight: 600;
      background: var(--accent-soft, rgba(127, 127, 127, 0.18));
    }
    .cp-source {
      flex: 1 1 auto; min-width: 0; font-size: 0.85rem; font-weight: 500;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    }
    a.cp-source { color: var(--link-color, #3b82f6); text-decoration: none; }
    a.cp-source:hover { text-decoration: underline; }
    .cp-claim {
      margin-top: 0.25rem; font-size: 0.8rem; opacity: 0.82; line-height: 1.35;
    }
    .cp-claim--withdrawn { text-decoration: line-through; }
    .cp-reason {
      margin-top: 0.2rem; font-size: 0.75rem; line-height: 1.35;
      color: var(--text-secondary);
    }
    .cp-item--focus {
      background: color-mix(in srgb, var(--accent-color) 10%, transparent);
      border-radius: var(--radius-tag);
    }
    .cp-cloud {
      margin-top: 0.4rem; display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap;
    }
    .cp-muted { font-size: 0.75rem; opacity: 0.7; }
    .cp-link {
      background: none; border: none; cursor: pointer; padding: 0;
      display: inline-flex; align-items: center; gap: 0.2rem;
      font-size: 0.78rem; color: var(--link-color, #3b82f6);
    }
    .cp-link:hover { text-decoration: underline; }
  `,
    ],
})
export class CitationsPanelComponent {
    private readonly chat = inject(PersistentChatService);
    private readonly doc = inject(DOCUMENT);

    /** The citation a chip's "show in citations list" asked for. */
    readonly focusedId = this.chat.citationFocus.asReadonly();

    constructor() {
        // Scroll the focused citation into view once its row has rendered.
        effect(() => {
            const cid = this.focusedId();
            this.rows();
            if (cid === null) return;
            untracked(() =>
                queueMicrotask(() => {
                    const row = this.doc.querySelector(`.cp-item[data-cid="${cid}"]`);
                    row?.scrollIntoView?.({block: 'nearest'});
                }),
            );
        });
    }

    /** Emitted when the user closes the panel. */
    readonly close = output<void>();

    private readonly driftByCid = signal<Map<number, DriftState>>(new Map());

    readonly rows = computed<CitationRow[]>(() => {
        const list = [...this.chat.citationsByCid().values()].sort((a, b) => a.id - b.id);
        return list.map((c, i) => ({
            ...c,
            index: i + 1,
            isWebLink: !!c.source_identifier && /^https?:\/\//i.test(c.source_identifier),
            state: citationState(c),
            reasonKey: citationReasonLabel(c.verification_reason),
        }));
    });

    driftOf(cid: number): DriftState {
        return this.driftByCid().get(cid);
    }

    async checkDrift(cid: number): Promise<void> {
        if (this.driftByCid().get(cid)) {
            return; // already loading or loaded
        }
        this.setDrift(cid, 'loading');
        const res = await this.chat.fetchCitationDrift(cid);
        this.setDrift(
            cid,
            res ?? {citation_id: cid, live_state: 'unknown', snapshot_available: false},
        );
    }

    async viewOriginal(cid: number): Promise<void> {
        await openCitationSnapshot(this.chat, cid);
    }

    stateTone(state: CitationState): BadgeTone {
        switch (state) {
            case 'verified':
                return 'success';
            case 'failed':
                return 'warning';
            default:
                return 'neutral';
        }
    }

    stateLabel(state: CitationState): string {
        return citationStateLabel(state);
    }

    driftTone(state: CitationDrift['live_state']): BadgeTone {
        switch (state) {
            case 'unchanged':
                return 'success';
            case 'changed':
                return 'alert';
            default:
                return 'neutral';
        }
    }

    driftLabel(state: CitationDrift['live_state']): string {
        switch (state) {
            case 'unchanged':
                return 'chat.citations.driftUnchanged';
            case 'changed':
                return 'chat.citations.driftChanged';
            case 'unreachable':
                return 'chat.citations.driftUnreachable';
            default:
                return 'chat.citations.driftUnknown';
        }
    }

    private setDrift(cid: number, value: DriftState): void {
        this.driftByCid.update((m) => {
            const next = new Map(m);
            next.set(cid, value);
            return next;
        });
    }
}
