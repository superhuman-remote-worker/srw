import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';

import {PersistentChatService, ThreadCitation} from '../services/persistent-chat.service';
import {WorkspaceFileDownloadService} from '../services/workspace-file-download.service';
import {CitationPopover} from './citation-popover';
import {citationPage, citationWorkspacePath} from './citation-status';

function citation(over: Partial<ThreadCitation> = {}): ThreadCitation {
    return {
        id: 5734,
        claim: 'The bill names Rotterdam.',
        source_id: 33426,
        source_name: '03_bill_of_lading.pdf',
        source_type: 'document',
        source_identifier: 'uploads/03_bill_of_lading.pdf',
        verification_status: 'verified',
        confidence: 'high',
        created_at: '2026-11-02T09:41:00Z',
        locator: {page: 1, section: 'Port of discharge'},
        ...over,
    };
}

describe('CitationPopover', () => {
    let files: {open: ReturnType<typeof vi.fn>};
    let anchor: HTMLElement;

    beforeEach(() => {
        files = {open: vi.fn().mockResolvedValue(true)};
        TestBed.configureTestingModule({
            providers: [
                {
                    provide: PersistentChatService,
                    useValue: {
                        threadId: signal('thread-1'),
                        citationFocus: signal<number | null>(null),
                        fetchCitationSnapshotBlob: vi.fn(),
                    },
                },
                {provide: WorkspaceFileDownloadService, useValue: files},
                {provide: TranslocoService, useValue: {translate: (key: string) => key}},
            ],
        });
        anchor = document.createElement('span');
        document.body.append(anchor);
    });

    afterEach(() => {
        TestBed.inject(CitationPopover).close();
        anchor.remove();
        vi.restoreAllMocks();
        TestBed.resetTestingModule();
    });

    function actions(c: ThreadCitation): HTMLButtonElement[] {
        TestBed.inject(CitationPopover).open(anchor, c, '2');
        return [...document.querySelectorAll<HTMLButtonElement>('.citation-popover .cpop-action')];
    }

    const labels = (buttons: HTMLButtonElement[]) => buttons.map((b) => b.textContent);

    it('opens a cited workspace document at its page', () => {
        const buttons = actions(citation());
        expect(labels(buttons)).toEqual([
            'chat.citations.popover.openSource',
            'chat.citations.popover.showInList',
        ]);

        buttons[0].click();

        expect(files.open).toHaveBeenCalledWith('thread-1', 'uploads/03_bill_of_lading.pdf', {page: 1});
        expect(document.querySelector('.citation-popover')).toBeNull();
    });

    it('opens a web source by its URL', () => {
        const open = vi.spyOn(window, 'open').mockReturnValue(null);
        const buttons = actions(
            citation({source_type: 'website', source_identifier: 'https://example.com/ucp600'}),
        );

        buttons[0].click();

        expect(open).toHaveBeenCalledWith('https://example.com/ucp600', '_blank', 'noopener');
        expect(files.open).not.toHaveBeenCalled();
    });

    it('offers the backed-up original, not the live file, for a snapshotted cloud document', () => {
        expect(labels(actions(citation({has_snapshot: true})))).toEqual([
            'chat.citations.viewOriginal',
            'chat.citations.popover.showInList',
        ]);
    });

    it('offers nothing to open for a pre-D2 temporary path', () => {
        expect(labels(actions(citation({source_identifier: '/tmp/tmpab12.pdf'})))).toEqual([
            'chat.citations.popover.showInList',
        ]);
    });
});

describe('citationWorkspacePath', () => {
    it('accepts a relative document path and normalizes it', () => {
        expect(citationWorkspacePath(citation({source_identifier: './uploads//a.pdf'}))).toBe('uploads/a.pdf');
    });

    it('rejects web pages, other source types, absolute paths, schemes and traversal', () => {
        expect(citationWorkspacePath(citation({source_type: 'website'}))).toBeNull();
        expect(citationWorkspacePath(citation({source_type: 'database'}))).toBeNull();
        expect(citationWorkspacePath(citation({source_identifier: '/tmp/x.pdf'}))).toBeNull();
        expect(citationWorkspacePath(citation({source_identifier: 'https://x.example/a.pdf'}))).toBeNull();
        expect(citationWorkspacePath(citation({source_identifier: 'C:\\x.pdf'}))).toBeNull();
        expect(citationWorkspacePath(citation({source_identifier: '../secret.pdf'}))).toBeNull();
        expect(citationWorkspacePath(citation({source_identifier: null}))).toBeNull();
    });
});

describe('citationPage', () => {
    it('reads a positive integer page, numeric or string', () => {
        expect(citationPage({page: 3})).toBe(3);
        expect(citationPage({page: '7'})).toBe(7);
        expect(citationPage({page: 0})).toBeUndefined();
        expect(citationPage({page: 'iv'})).toBeUndefined();
        expect(citationPage({section: 'A'})).toBeUndefined();
        expect(citationPage(null)).toBeUndefined();
    });
});
