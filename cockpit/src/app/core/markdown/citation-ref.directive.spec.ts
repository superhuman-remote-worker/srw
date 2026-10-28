import {Component, signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {MARKED_EXTENSIONS, SANITIZE, MarkdownComponent, provideMarkdown} from 'ngx-markdown';
import {afterEach, beforeEach, describe, expect, it} from 'vitest';

import {PersistentChatService, ThreadCitation} from '../services/persistent-chat.service';
import {citationExtension} from './citation-extension';
import {CitationRefDirective} from './citation-ref.directive';
import {CitationScope, citationIdsIn} from './citation-scope';
import {sanitizeMarkdownHtml} from './markdown-sanitizer';

@Component({
    standalone: true,
    imports: [MarkdownComponent, CitationRefDirective],
    template: `<markdown appCitationRef [data]="content" [citationScope]="scope"></markdown>`,
})
class Host {
    content = '';
    scope: CitationScope | null = null;
}

function citation(over: Partial<ThreadCitation>): ThreadCitation {
    return {
        id: 1,
        claim: 'A claim',
        source_id: 10,
        source_name: 'Example Source',
        source_type: 'website',
        source_identifier: 'https://example.com/page',
        verification_status: 'verified',
        confidence: 'high',
        created_at: '2026-10-28T00:00:00Z',
        ...over,
    };
}

describe('CitationRefDirective', () => {
    const citationsByCid = signal(new Map<number, ThreadCitation>());
    const citationsLoaded = signal(true);
    const citationFocus = signal<number | null>(null);

    beforeEach(() => {
        citationsByCid.set(new Map());
        citationsLoaded.set(true);
        TestBed.configureTestingModule({
            imports: [Host],
            providers: [
                provideMarkdown({
                    markedExtensions: [{provide: MARKED_EXTENSIONS, multi: true, useValue: citationExtension()}],
                    sanitize: {provide: SANITIZE, useValue: sanitizeMarkdownHtml},
                }),
                {provide: PersistentChatService, useValue: {citationsByCid, citationsLoaded, citationFocus}},
                {provide: TranslocoService, useValue: {translate: (k: string) => k}},
            ],
        });
    });

    afterEach(() => {
        document.querySelectorAll('.citation-popover').forEach((el) => el.remove());
        TestBed.resetTestingModule();
    });

    async function render(content: string, scoped = true) {
        const fixture = TestBed.createComponent(Host);
        fixture.componentInstance.content = content;
        fixture.componentInstance.scope = scoped ? new CitationScope(citationIdsIn(content)) : null;
        fixture.detectChanges();
        await fixture.whenStable();
        await new Promise((resolve) => setTimeout(resolve, 0));
        fixture.detectChanges();
        return fixture;
    }

    const refs = (root: HTMLElement) => Array.from(root.querySelectorAll<HTMLElement>('.citation-ref'));

    it('numbers known handles by first mention and styles them by state', async () => {
        citationsByCid.set(
            new Map([
                [1241, citation({id: 1241, verification_status: 'pending'})],
                [1242, citation({id: 1242, verification_status: 'failed', verification_reason: 'quote_not_found'})],
            ]),
        );
        const fixture = await render('A [^c1242], B [^c1241], A again [^c1242].');
        const chips = refs(fixture.nativeElement);
        expect(chips.map((c) => c.textContent)).toEqual(['1', '2', '1']);
        expect(chips[0].classList).toContain('state-failed');
        expect(chips[1].classList).toContain('state-pending');
        expect(chips[0].getAttribute('role')).toBe('button');
        fixture.destroy();
    });

    it('flags an unknown handle instead of hiding it, and keeps a stray [5] as text', async () => {
        const fixture = await render('Unknown [^c99] and a list item [5].');
        const [handle, legacy] = refs(fixture.nativeElement);
        expect(handle.classList).toContain('flagged');
        expect(handle.textContent).toBe('?');
        expect(legacy.classList).toContain('unresolved');
        expect(legacy.textContent).toBe('[5]');
        fixture.destroy();
    });

    it('resolves legacy [N] markers of the session (D5)', async () => {
        citationsByCid.set(new Map([[1276, citation({id: 1276})]]));
        const fixture = await render('Old session claim [1276].');
        const [chip] = refs(fixture.nativeElement);
        expect(chip.textContent).toBe('1');
        expect(chip.classList).toContain('state-verified');
        fixture.destroy();
    });

    it('outside an answer leaves unknown handles and legacy markers as plain text', async () => {
        citationsByCid.set(new Map([[7, citation({id: 7})]]));
        const fixture = await render('Known [^c7], unknown [^c8], legacy [7].', false);
        const [known, unknown, legacy] = refs(fixture.nativeElement);
        expect(known.classList).toContain('resolved');
        expect(unknown.textContent).toBe('[^c8]');
        expect(unknown.classList).not.toContain('flagged');
        expect(legacy.textContent).toBe('[7]');
        fixture.destroy();
    });

    it('a chip upgrades when its citation arrives live', async () => {
        citationsLoaded.set(false);
        const fixture = await render('Claim [^c50].');
        expect(refs(fixture.nativeElement)[0].classList).toContain('loading');
        citationsByCid.set(new Map([[50, citation({id: 50, verification_status: 'pending'})]]));
        citationsLoaded.set(true);
        fixture.detectChanges();
        const [chip] = refs(fixture.nativeElement);
        expect(chip.textContent).toBe('1');
        expect(chip.classList).toContain('state-pending');
        fixture.destroy();
    });

    it('opens a popover with the claim, the passage and the reason', async () => {
        citationsByCid.set(
            new Map([
                [
                    3,
                    citation({
                        id: 3,
                        verification_status: 'failed',
                        verification_reason: 'not_supported',
                        verification_notes: 'says something else',
                        matched_text: 'the matched passage',
                        locator: {page: 4},
                    }),
                ],
            ]),
        );
        const fixture = await render('Claim [^c3].');
        refs(fixture.nativeElement)[0].click();
        const pop = document.querySelector<HTMLElement>('.citation-popover');
        expect(pop).not.toBeNull();
        expect(pop!.textContent).toContain('A claim');
        expect(pop!.textContent).toContain('the matched passage');
        expect(pop!.textContent).toContain('chat.citations.reason.not_supported');
        expect(pop!.textContent).toContain('says something else');
        expect(pop!.textContent).toContain('p. 4');
        // "Show in citations list" focuses the panel row.
        const show = Array.from(pop!.querySelectorAll('button')).find(
            (b) => b.textContent === 'chat.citations.popover.showInList',
        );
        show!.click();
        expect(citationFocus()).toBe(3);
        expect(document.querySelector('.citation-popover')).toBeNull();
        fixture.destroy();
    });
});
