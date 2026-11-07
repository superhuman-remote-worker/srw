import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';

import {CitationsPanelComponent, snapshotViewType} from './citations-panel.component';
import {
    PersistentChatService,
    ThreadCitation,
} from '../../../core/services/persistent-chat.service';
import {WorkspaceFileDownloadService} from '../../../core/services/workspace-file-download.service';

function citation(over: Partial<ThreadCitation> = {}): ThreadCitation {
    return {
        id: 1,
        claim: 'A claim',
        source_id: 10,
        source_name: 'Example Source',
        source_type: 'website',
        source_identifier: 'https://example.com',
        verification_status: 'verified',
        confidence: 'high',
        created_at: '2026-06-24T00:00:00Z',
        ...over,
    };
}

describe('CitationsPanelComponent', () => {
    let citationsByCid: ReturnType<typeof signal<Map<number, ThreadCitation>>>;
    let mockChat: {
        citationsByCid: typeof citationsByCid;
        citationFocus: ReturnType<typeof signal<number | null>>;
        threadId: ReturnType<typeof signal<string | null>>;
        fetchCitationDrift: ReturnType<typeof vi.fn>;
        fetchCitationSnapshotBlob: ReturnType<typeof vi.fn>;
    };
    let files: {open: ReturnType<typeof vi.fn>};

    beforeEach(() => {
        citationsByCid = signal(new Map<number, ThreadCitation>());
        mockChat = {
            citationsByCid,
            citationFocus: signal<number | null>(null),
            threadId: signal<string | null>('thread-1'),
            fetchCitationDrift: vi.fn(),
            fetchCitationSnapshotBlob: vi.fn(),
        };
        files = {open: vi.fn().mockResolvedValue(true)};
        TestBed.resetTestingModule();
        TestBed.configureTestingModule({
            providers: [
                {provide: PersistentChatService, useValue: mockChat},
                {provide: WorkspaceFileDownloadService, useValue: files},
            ],
        });
    });

    afterEach(() => {
        // Restore window.open etc. — vi.spyOn returns the same spy on repeat, so
        // call history would otherwise leak between tests.
        vi.restoreAllMocks();
    });

    function make(): CitationsPanelComponent {
        return TestBed.runInInjectionContext(
            () => new CitationsPanelComponent(),
        ) as CitationsPanelComponent;
    }

    it('rows sorts by id ascending and numbers them 1..k', () => {
        citationsByCid.set(
            new Map([
                [24, citation({id: 24})],
                [12, citation({id: 12})],
            ]),
        );
        const rows = make().rows();
        expect(rows.map((r) => r.id)).toEqual([12, 24]);
        expect(rows.map((r) => r.index)).toEqual([1, 2]);
    });

    it('flags only http(s) sources as web links', () => {
        citationsByCid.set(
            new Map([
                [1, citation({id: 1, source_identifier: 'https://example.com'})],
                [2, citation({id: 2, source_identifier: '/Personal/doc.pdf'})],
                [3, citation({id: 3, source_identifier: null})],
            ]),
        );
        const rows = make().rows();
        expect(rows.find((r) => r.id === 1)!.isWebLink).toBe(true);
        expect(rows.find((r) => r.id === 2)!.isWebLink).toBe(false);
        expect(rows.find((r) => r.id === 3)!.isWebLink).toBe(false);
    });

    it('gives a workspace document "open source", but not a web page, a snapshot or an old temp path', () => {
        const doc = {source_type: 'document', source_identifier: 'uploads/credit.pdf'};
        citationsByCid.set(
            new Map([
                [1, citation({id: 1, ...doc, locator: {page: 3}})],
                [2, citation({id: 2})],
                [3, citation({id: 3, ...doc, has_snapshot: true})],
                [4, citation({id: 4, source_type: 'document', source_identifier: '/tmp/tmpab12.pdf'})],
            ]),
        );
        const panel = make();
        const rows = panel.rows();
        expect(rows.map((r) => r.workspacePath)).toEqual(['uploads/credit.pdf', null, null, null]);

        panel.openSource(rows[0]);
        expect(files.open).toHaveBeenCalledWith('thread-1', 'uploads/credit.pdf', {page: 3});
    });

    it('maps citation states to badge tones + labels; pending never looks verified', () => {
        const c = make();
        expect(c.stateTone('verified')).toBe('success');
        expect(c.stateTone('failed')).toBe('warning');
        expect(c.stateTone('pending')).toBe('neutral');
        expect(c.stateTone('unchecked')).toBe('neutral');
        expect(c.stateLabel('failed')).toBe('chat.citations.state.failed');
        expect(c.stateLabel('unchecked')).toBe('chat.citations.state.unchecked');
    });

    it('derives state and reason per row; withdrawn wins and stays listed', () => {
        citationsByCid.set(
            new Map([
                [1, citation({id: 1, verification_status: 'failed', verification_reason: 'quote_not_found'})],
                [2, citation({id: 2, verification_status: 'unverified', verification_reason: 'not_checked'})],
                [3, citation({id: 3, verification_status: 'failed', withdrawn_at: '2026-10-28T00:00:00Z'})],
                [4, citation({id: 4, verification_status: 'pending'})],
            ]),
        );
        const rows = make().rows();
        expect(rows.map((r) => r.state)).toEqual(['failed', 'unchecked', 'withdrawn', 'pending']);
        expect(rows[0].reasonKey).toBe('chat.citations.reason.quote_not_found');
        expect(rows[1].reasonKey).toBe('chat.citations.reason.not_checked');
        expect(rows[3].reasonKey).toBeNull();
    });

    it('maps drift state to badge tones + labels', () => {
        const c = make();
        expect(c.driftTone('unchanged')).toBe('success');
        expect(c.driftTone('changed')).toBe('alert');
        expect(c.driftTone('unreachable')).toBe('neutral');
        expect(c.driftLabel('unreachable')).toBe('chat.citations.driftUnreachable');
    });

    it('checkDrift fetches once, caches, and is a no-op on repeat', async () => {
        mockChat.fetchCitationDrift.mockResolvedValue({
            citation_id: 5,
            live_state: 'changed',
            snapshot_available: true,
        });
        const c = make();
        await c.checkDrift(5);
        expect(mockChat.fetchCitationDrift).toHaveBeenCalledWith(5);
        expect(c.driftOf(5)).toMatchObject({live_state: 'changed'});

        await c.checkDrift(5);
        expect(mockChat.fetchCitationDrift).toHaveBeenCalledTimes(1);
    });

    it('checkDrift falls back to unknown when the fetch returns null', async () => {
        mockChat.fetchCitationDrift.mockResolvedValue(null);
        const c = make();
        await c.checkDrift(5);
        expect(c.driftOf(5)).toMatchObject({live_state: 'unknown'});
    });

    /** Stub the object-URL API (jsdom has none) and return the blobs it was given. */
    function stubObjectUrls(): Blob[] {
        const made: Blob[] = [];
        (URL as unknown as {createObjectURL: unknown}).createObjectURL = vi.fn((b: Blob) => {
            made.push(b);
            return 'blob:abc';
        });
        (URL as unknown as {revokeObjectURL: unknown}).revokeObjectURL = vi.fn();
        return made;
    }

    /** jsdom's Blob has no text(); FileReader it is. */
    function readText(blob: Blob): Promise<string> {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result as string);
            reader.onerror = () => reject(reader.error);
            reader.readAsText(blob);
        });
    }

    /** Capture anchor clicks instead of letting jsdom try to navigate. */
    function captureAnchorClicks(): HTMLAnchorElement[] {
        const clicked: HTMLAnchorElement[] = [];
        vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
            this: HTMLAnchorElement,
        ) {
            clicked.push(this);
        });
        return clicked;
    }

    it('viewOriginal opens a PDF snapshot as a PDF', async () => {
        mockChat.fetchCitationSnapshotBlob.mockResolvedValue(
            new Blob(['%PDF-1.7'], {type: 'application/pdf'}),
        );
        const openSpy = vi.spyOn(window, 'open').mockReturnValue(null);
        const made = stubObjectUrls();
        const clicked = captureAnchorClicks();

        await make().viewOriginal(9);

        expect(mockChat.fetchCitationSnapshotBlob).toHaveBeenCalledWith(9);
        expect(made).toHaveLength(1);
        expect(made[0].type).toBe('application/pdf');
        expect(await readText(made[0])).toBe('%PDF-1.7');
        expect(openSpy).toHaveBeenCalledWith('blob:abc', '_blank');
        expect(clicked).toHaveLength(0);
    });

    it('viewOriginal opens an HTML snapshot as plain text, never as HTML', async () => {
        // A blob: URL inherits the Cockpit origin: opened as text/html this
        // would run the cited page's script with the reader's session.
        const html = '<script>alert(document.domain)</script>';
        mockChat.fetchCitationSnapshotBlob.mockResolvedValue(
            new Blob([html], {type: 'text/html; charset=utf-8'}),
        );
        const openSpy = vi.spyOn(window, 'open').mockReturnValue(null);
        const made = stubObjectUrls();

        await make().viewOriginal(9);

        expect(made).toHaveLength(1);
        expect(made[0].type).toBe('text/plain;charset=utf-8');
        expect(await readText(made[0])).toBe(html);
        expect(openSpy).toHaveBeenCalledWith('blob:abc', '_blank');
    });

    it('viewOriginal downloads an SVG snapshot instead of opening it', async () => {
        citationsByCid.set(
            new Map([[9, citation({id: 9, source_name: '/Personal/diagram.svg'})]]),
        );
        mockChat.fetchCitationSnapshotBlob.mockResolvedValue(
            new Blob(['<svg onload="alert(1)"/>'], {type: 'image/svg+xml'}),
        );
        const openSpy = vi.spyOn(window, 'open').mockReturnValue(null);
        const made = stubObjectUrls();
        const clicked = captureAnchorClicks();

        await make().viewOriginal(9);

        expect(openSpy).not.toHaveBeenCalled();
        expect(made).toHaveLength(1);
        expect(made[0].type).toBe('application/octet-stream');
        expect(clicked).toHaveLength(1);
        expect(clicked[0].getAttribute('href')).toBe('blob:abc');
        expect(clicked[0].download).toBe('diagram.svg');
        expect(URL.revokeObjectURL).not.toHaveBeenCalled(); // kept for the 60 s window
    });

    it('viewOriginal downloads an untyped snapshot under a fallback name', async () => {
        mockChat.fetchCitationSnapshotBlob.mockResolvedValue(new Blob(['x']));
        const openSpy = vi.spyOn(window, 'open').mockReturnValue(null);
        stubObjectUrls();
        const clicked = captureAnchorClicks();

        await make().viewOriginal(4);

        expect(openSpy).not.toHaveBeenCalled();
        expect(clicked[0].download).toBe('citation-4');
    });

    it('snapshotViewType allowlists inert types and maps text/* to plain text', () => {
        expect(snapshotViewType('application/pdf')).toBe('application/pdf');
        expect(snapshotViewType('IMAGE/PNG')).toBe('image/png');
        expect(snapshotViewType('image/jpeg')).toBe('image/jpeg');
        expect(snapshotViewType('image/gif')).toBe('image/gif');
        expect(snapshotViewType('image/webp')).toBe('image/webp');
        expect(snapshotViewType('text/plain')).toBe('text/plain;charset=utf-8');
        expect(snapshotViewType('text/xml;charset=ISO-8859-1')).toBe(
            'text/plain;charset=ISO-8859-1',
        );
        expect(snapshotViewType('text/html;charset="<x>"')).toBe('text/plain;charset=utf-8');
        expect(snapshotViewType('image/svg+xml')).toBeNull();
        expect(snapshotViewType('application/xhtml+xml')).toBeNull();
        expect(snapshotViewType('application/xml')).toBeNull();
        expect(snapshotViewType('application/octet-stream')).toBeNull();
        expect(snapshotViewType('')).toBeNull();
    });

    it('viewOriginal is a no-op when no snapshot is available', async () => {
        mockChat.fetchCitationSnapshotBlob.mockResolvedValue(null);
        const openSpy = vi.spyOn(window, 'open').mockReturnValue(null);
        await make().viewOriginal(9);
        expect(openSpy).not.toHaveBeenCalled();
    });
});
