import {DOCUMENT, Injectable, inject} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';

import {PersistentChatService, ThreadCitation} from '../services/persistent-chat.service';
import {
    citationLocatorText,
    citationReasonLabel,
    citationSourceUrl,
    citationState,
    citationStateLabel,
} from './citation-status';
import {openCitationSnapshot} from './citation-snapshot';

/**
 * The popover a citation chip opens (overhaul §6.2): the source, where in it,
 * the state with its reason, the claim, and the passage the check found (the
 * evidence, not only the verdict), with "open source", "view original" and
 * "show in the citations list". One popover at a time, rendered to
 * `document.body` like the tooltip; built with text nodes only, since every
 * field is agent- or source-controlled.
 */
@Injectable({providedIn: 'root'})
export class CitationPopover {
    private readonly doc = inject(DOCUMENT);
    private readonly chat = inject(PersistentChatService);
    private readonly i18n = inject(TranslocoService);

    private el: HTMLElement | null = null;
    private anchor: HTMLElement | null = null;

    private readonly onPointerDown = (ev: Event): void => {
        const target = ev.target as Node | null;
        if (this.el?.contains(target) || this.anchor?.contains(target)) return;
        this.close();
    };
    private readonly onKey = (ev: KeyboardEvent): void => {
        if (ev.key === 'Escape') {
            const anchor = this.anchor;
            this.close();
            anchor?.focus();
        }
    };
    private readonly onScroll = (ev: Event): void => {
        if (this.el && !this.el.contains(ev.target as Node)) this.close();
    };

    /** Whether the popover is open on `anchor`. */
    isOpenOn(anchor: HTMLElement): boolean {
        return this.anchor === anchor && !!this.el;
    }

    open(anchor: HTMLElement, citation: ThreadCitation, displayNumber: string): void {
        this.close();
        const t = (key: string, params?: Record<string, unknown>) => this.i18n.translate(key, params);
        const el = this.doc.createElement('div');
        el.className = 'citation-popover';
        el.setAttribute('role', 'dialog');
        el.setAttribute('aria-label', t('chat.citations.popover.label', {n: displayNumber}));

        const state = citationState(citation);
        const head = this.node('div', 'cpop-head');
        head.append(this.node('span', `cpop-num citation-ref resolved state-${state}`, displayNumber));
        const url = citationSourceUrl(citation);
        const sourceName = citation.source_name || t('chat.citations.popover.unknownSource');
        if (url) {
            const a = this.node('a', 'cpop-source', sourceName) as HTMLAnchorElement;
            a.href = url;
            a.target = '_blank';
            a.rel = 'noopener noreferrer';
            head.append(a);
        } else {
            head.append(this.node('span', 'cpop-source', sourceName));
        }
        head.append(this.node('span', `cpop-state cpop-state--${state}`, t(citationStateLabel(state))));
        el.append(head);

        const where = citationLocatorText(citation.locator);
        if (where) el.append(this.node('div', 'cpop-where', where));

        if (state === 'pending') {
            el.append(this.node('div', 'cpop-note', t('chat.citations.popover.pendingNote')));
        } else if (state === 'failed' || state === 'unchecked') {
            const reasonKey = citationReasonLabel(citation.verification_reason);
            const reason = this.node('div', `cpop-note cpop-note--${state}`);
            if (reasonKey) reason.append(this.node('strong', '', t(reasonKey)));
            if (citation.verification_notes) {
                reason.append(this.node('span', '', (reasonKey ? ' — ' : '') + citation.verification_notes));
            }
            if (reason.childNodes.length) el.append(reason);
        } else if (state === 'withdrawn') {
            el.append(
                this.node(
                    'div',
                    'cpop-note cpop-note--withdrawn',
                    t('chat.citations.popover.withdrawn', {
                        reason: citation.withdrawn_reason || t('chat.citations.popover.noReason'),
                    }),
                ),
            );
        }

        if (citation.claim) {
            el.append(this.field(t('chat.citations.popover.claim'), citation.claim));
        }
        const passage = citation.matched_text || citation.quote;
        if (passage) {
            el.append(this.field(t('chat.citations.popover.passage'), `“${passage}”`, 'cpop-passage'));
        }

        const actions = this.node('div', 'cpop-actions');
        if (url) {
            const open = this.button(t('chat.citations.popover.openSource'), () => {
                window.open(url, '_blank', 'noopener');
                this.close();
            });
            actions.append(open);
        }
        if (citation.has_snapshot) {
            actions.append(
                this.button(t('chat.citations.viewOriginal'), () => {
                    void openCitationSnapshot(this.chat, citation.id);
                    this.close();
                }),
            );
        }
        actions.append(
            this.button(t('chat.citations.popover.showInList'), () => {
                this.chat.citationFocus.set(citation.id);
                this.close();
            }),
        );
        el.append(actions);

        this.doc.body.append(el);
        this.el = el;
        this.anchor = anchor;
        anchor.setAttribute('aria-expanded', 'true');
        this.position();
        this.doc.addEventListener('pointerdown', this.onPointerDown, true);
        this.doc.addEventListener('keydown', this.onKey, true);
        this.doc.defaultView?.addEventListener('scroll', this.onScroll, true);
        this.doc.defaultView?.addEventListener('resize', this.onScroll, true);
    }

    close(): void {
        if (!this.el) return;
        this.el.remove();
        this.anchor?.setAttribute('aria-expanded', 'false');
        this.el = null;
        this.anchor = null;
        this.doc.removeEventListener('pointerdown', this.onPointerDown, true);
        this.doc.removeEventListener('keydown', this.onKey, true);
        this.doc.defaultView?.removeEventListener('scroll', this.onScroll, true);
        this.doc.defaultView?.removeEventListener('resize', this.onScroll, true);
    }

    /** Close the popover when it belongs to a chip inside `root` (re-render, destroy). */
    closeWithin(root: HTMLElement): void {
        if (this.anchor && root.contains(this.anchor)) this.close();
    }

    private position(): void {
        const el = this.el;
        const anchor = this.anchor;
        const view = this.doc.defaultView;
        if (!el || !anchor || !view) return;
        const gutter = 8;
        const rect = anchor.getBoundingClientRect();
        const width = Math.min(360, view.innerWidth - 2 * gutter);
        el.style.width = `${width}px`;
        const left = Math.max(gutter, Math.min(rect.left, view.innerWidth - width - gutter));
        el.style.left = `${left}px`;
        const below = rect.bottom + 6;
        const height = el.offsetHeight;
        const top = below + height > view.innerHeight - gutter && rect.top - height - 6 > gutter
            ? rect.top - height - 6
            : below;
        el.style.top = `${top}px`;
    }

    private node(tag: string, className: string, text?: string): HTMLElement {
        const n = this.doc.createElement(tag);
        if (className) n.className = className;
        if (text !== undefined) n.textContent = text;
        return n;
    }

    private field(label: string, value: string, className = ''): HTMLElement {
        const wrap = this.node('div', `cpop-field ${className}`.trim());
        wrap.append(this.node('div', 'cpop-label', label));
        wrap.append(this.node('div', 'cpop-value', value));
        return wrap;
    }

    private button(label: string, run: () => void): HTMLElement {
        const b = this.node('button', 'cpop-action', label) as HTMLButtonElement;
        b.type = 'button';
        b.addEventListener('click', (ev) => {
            ev.preventDefault();
            ev.stopPropagation();
            run();
        });
        return b;
    }
}
