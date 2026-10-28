import {AfterViewInit, Directive, ElementRef, Input, OnDestroy, effect, inject, signal} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';
import {MarkdownComponent} from 'ngx-markdown';
import {Subscription} from 'rxjs';

import {PersistentChatService} from '../services/persistent-chat.service';
import {CitationPopover} from './citation-popover';
import {CitationScope, citationIdsIn} from './citation-scope';
import {citationState, citationStateLabel} from './citation-status';

/**
 * Resolves the citation handles `citationExtension` marks into numbered chips
 * with a popover (citation engine overhaul §6.2).
 *
 * The marked stage emits `<span class="citation-ref cid-N handle|legacy">` with
 * the literal handle as its text. After render — and again whenever the
 * session's citations change (they arrive live, before the agent writes the
 * handle) — each marker becomes:
 *
 *   - a known citation → a chip numbered by first mention across the answer
 *     (`citationScope`; without one, across this block), styled by state
 *     (being checked, verified, unverified, not checked, withdrawn), opening
 *     the popover on click or Enter;
 *   - an unknown `[^c…]` handle in an answer → a flagged chip, never hidden: a
 *     hidden one can't be told apart from a real bug. Outside an answer it
 *     stays plain text;
 *   - an unknown legacy `[N]` → its literal text, since a stray `[5]` in prose
 *     is no citation. Legacy markers resolve only with a scope, i.e. inside
 *     the session chat whose citations they name.
 *
 * Non-destructive: the `cid-N` class stays, so a marker upgrades when its
 * citation loads later. The display number never reaches the model's history.
 */
@Directive({
    selector: 'markdown[appCitationRef]',
    standalone: true,
})
export class CitationRefDirective implements AfterViewInit, OnDestroy {
    /**
     * The answer this block belongs to; numbers chips across all its blocks.
     * A decorator input, not `input()`: an initializer input on a directive
     * does not bind under the specs' JIT compile.
     */
    @Input() set citationScope(value: CitationScope | null | undefined) {
        this.scope.set(value ?? null);
    }
    private readonly scope = signal<CitationScope | null>(null);

    private readonly host = inject<ElementRef<HTMLElement>>(ElementRef);
    private readonly markdown = inject(MarkdownComponent, {self: true});
    private readonly chat = inject(PersistentChatService);
    private readonly popover = inject(CitationPopover);
    private readonly i18n = inject(TranslocoService);
    private sub?: Subscription;

    constructor() {
        // Re-resolve when the thread's citations change after render.
        effect(() => {
            this.chat.citationsByCid();
            this.chat.citationsLoaded();
            this.scope();
            this.resolve();
        });
    }

    ngAfterViewInit(): void {
        this.resolve();
        this.sub = this.markdown.ready.subscribe(() => {
            this.popover.closeWithin(this.host.nativeElement);
            this.resolve();
        });
        this.host.nativeElement.addEventListener('click', this.onClick);
        this.host.nativeElement.addEventListener('keydown', this.onKeydown);
    }

    ngOnDestroy(): void {
        this.sub?.unsubscribe();
        this.popover.closeWithin(this.host.nativeElement);
        this.host.nativeElement.removeEventListener('click', this.onClick);
        this.host.nativeElement.removeEventListener('keydown', this.onKeydown);
    }

    private readonly onClick = (ev: Event): void => {
        const chip = (ev.target as HTMLElement | null)?.closest('.citation-ref.resolved') as HTMLElement | null;
        if (!chip) return;
        ev.preventDefault();
        this.toggle(chip);
    };

    private readonly onKeydown = (ev: KeyboardEvent): void => {
        if (ev.key !== 'Enter' && ev.key !== ' ') return;
        const chip = (ev.target as HTMLElement | null)?.closest('.citation-ref.resolved') as HTMLElement | null;
        if (!chip) return;
        ev.preventDefault();
        this.toggle(chip);
    };

    private toggle(chip: HTMLElement): void {
        if (this.popover.isOpenOn(chip)) {
            this.popover.close();
            return;
        }
        const cit = this.chat.citationsByCid().get(this.cidOf(chip));
        if (cit) this.popover.open(chip, cit, chip.textContent ?? '');
    }

    private cidOf(el: HTMLElement): number {
        const m = el.className.match(/\bcid-(\d+)\b/);
        return m ? Number(m[1]) : NaN;
    }

    private resolve(): void {
        const refs = this.host.nativeElement.querySelectorAll<HTMLElement>('.citation-ref');
        if (refs.length === 0) {
            return;
        }
        const byCid = this.chat.citationsByCid();
        const loaded = this.chat.citationsLoaded();
        const scope = this.scope();
        // Without an answer scope, number across this block alone.
        const numbering = scope ?? new CitationScope(citationIdsIn(this.markdown.data ?? ''));
        refs.forEach((el) => {
            const cid = this.cidOf(el);
            if (!Number.isFinite(cid)) {
                return;
            }
            const legacy = el.classList.contains('legacy');
            const kind = legacy ? 'legacy' : 'handle';
            const cit = legacy && !scope ? undefined : byCid.get(cid);
            if (cit) {
                const n = String(numbering.numberOf(cid, byCid) ?? '?');
                const state = citationState(cit);
                el.textContent = n;
                el.className = `citation-ref cid-${cid} ${kind} resolved state-${state}`;
                el.setAttribute('role', 'button');
                el.setAttribute('tabindex', '0');
                el.setAttribute('aria-haspopup', 'dialog');
                el.setAttribute(
                    'aria-label',
                    this.i18n.translate('chat.citations.chipLabel', {
                        n,
                        source: cit.source_name || '',
                        state: this.i18n.translate(citationStateLabel(state)),
                    }),
                );
                el.removeAttribute('title');
                return;
            }
            this.clearChip(el);
            if (legacy || !scope) {
                // Not a citation this view knows: the literal text it was.
                // Outside an answer (a job's tool card) an unknown handle is
                // not flagged: this chat's citations are not that job's.
                el.textContent = legacy ? `[${cid}]` : `[^c${cid}]`;
                el.className = `citation-ref cid-${cid} ${kind} unresolved`;
            } else if (loaded) {
                el.textContent = '?';
                el.className = `citation-ref cid-${cid} handle flagged`;
                el.setAttribute('title', this.i18n.translate('chat.citations.notFound', {handle: `[^c${cid}]`}));
            } else {
                // Citations still loading: a quiet placeholder, not the raw handle.
                el.textContent = '…';
                el.className = `citation-ref cid-${cid} handle loading`;
            }
        });
    }

    private clearChip(el: HTMLElement): void {
        el.removeAttribute('role');
        el.removeAttribute('tabindex');
        el.removeAttribute('aria-haspopup');
        el.removeAttribute('aria-label');
        el.removeAttribute('aria-expanded');
        el.removeAttribute('title');
    }
}
