import type {PersistentChatService} from '../services/persistent-chat.service';

/** Snapshot types that render without script when opened in a tab. */
const SNAPSHOT_VIEW_TYPES: ReadonlySet<string> = new Set([
    'application/pdf',
    'image/png',
    'image/jpeg',
    'image/gif',
    'image/webp',
]);

/**
 * The type a cited snapshot may be opened as, or null when it must be
 * downloaded instead.
 *
 * A `blob:` URL inherits the Cockpit origin, so a stored `text/html` or
 * `image/svg+xml` snapshot opened under its own type would run its script with
 * the reader's session. Only the allowlisted types open as themselves; every
 * `text/*` type, HTML included, opens as plain text so it shows as source.
 * SVG and everything else is not on the list.
 */
export function snapshotViewType(type: string): string | null {
    const [essence, ...params] = type.split(';');
    const mime = essence.trim().toLowerCase();
    if (SNAPSHOT_VIEW_TYPES.has(mime)) {
        return mime;
    }
    if (mime.startsWith('text/')) {
        const charset = params
            .map((p) => p.trim().match(/^charset=([A-Za-z0-9_.:-]+)$/i)?.[1])
            .find((c) => !!c);
        return `text/plain;charset=${charset ?? 'utf-8'}`;
    }
    return null;
}

/**
 * "View original": open (or download) the backed-up copy of a cited cloud
 * document. Shared by the citations panel and the chip popover.
 */
export async function openCitationSnapshot(chat: PersistentChatService, cid: number): Promise<void> {
    const blob = await chat.fetchCitationSnapshotBlob(cid);
    if (!blob) {
        return;
    }
    // Never open the blob under the type the server stored: see
    // snapshotViewType. A type that cannot be opened safely is downloaded,
    // re-wrapped as octet-stream so it cannot render even if navigated to.
    const viewType = snapshotViewType(blob.type);
    const url = URL.createObjectURL(new Blob([blob], {type: viewType ?? 'application/octet-stream'}));
    if (viewType) {
        window.open(url, '_blank');
    } else {
        const a = document.createElement('a');
        a.href = url;
        const name = chat.citationsByCid().get(cid)?.source_name ?? '';
        a.download = name.split('/').pop()?.trim() || `citation-${cid}`;
        a.click();
    }
    // Let the new tab or the download read it, then release the object URL.
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
}
