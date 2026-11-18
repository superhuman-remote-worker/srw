/**
 * Pure helpers for the outbox's upload stage.
 *
 * Lives outside persistent-chat.service.ts so the send/upload logic that
 * matters most is testable without instantiating a 4300-line service.
 * See knowledge-base/knowledge/features/session_attachment_send_flow.md §5.
 */
import type {ChatAttachment} from './persistent-chat.service';

/** One file queued on an outbox item, before or during its upload. */
export interface PendingUpload {
    /** FilePreview.id — a stable key that exists before the server path does. */
    id: string;
    file: File;
    name: string;
    size: number;
    mimeType: string;
    /** Bytes sent so far, as last reported by an XHR UploadProgress event. */
    loaded: number;
    /** Total bytes, or null when the browser cannot compute it. */
    total: number | null;
    status: 'queued' | 'uploading' | 'done' | 'failed';
    error?: string;
    /** Set once the server confirms; retries must never re-upload these. */
    resolved?: ChatAttachment;
}

/**
 * Uppy's identity key for a selected file. Two chips with the same key are the
 * same file, so the second is refused rather than attached.
 *
 * This matters more since eager upload (§5.4): the backend has no upload
 * idempotency — `_claim_name` resolves a collision with a `_1` suffix against a
 * live listing — so two chips for one file become two files in `uploads/`, and
 * the message hint names both. Rejecting the duplicate at the composer removes
 * the most common path to that at zero cost.
 *
 * Deliberately keyed off the `File`, not the `FilePreview`: a voice recording's
 * preview `name` is a human label ("Voice message (0:03)"), while its File
 * carries the real, timestamped filename.
 */
export function attachmentDedupeKey(file: {
    name: string;
    size: number;
    lastModified?: number;
}): string {
    return `${file.name}|${file.size}|${file.lastModified ?? 0}`;
}

/**
 * The smallest set of `uploads/`-relative paths whose deletion removes every
 * one of `names`.
 *
 * One upload can produce many entries: a `.zip` is expanded server-side into
 * `<stem>/a.txt`, `<stem>/sub/b.txt`, … Because the DELETE route removes a
 * named subtree, collapsing to the distinct top-level segments turns a
 * 100-member archive into one request. A plain file is its own top-level
 * segment, so the same code covers both.
 */
export function topLevelUploadTargets(names: readonly string[]): string[] {
    const targets = new Set<string>();
    for (const name of names) {
        const top = name.split('/')[0];
        if (top) targets.add(top);
    }
    return [...targets];
}

const RECORDING_HINT_PREFIX = '[Transcribed from a voice recording; the original audio is in uploads/: ';
const ATTACHMENT_HINT_RE = /(?:^|\n\n)\[Attached files in uploads\/: ([^\n]+)\]\s*$/;
const RECORDING_HINT_RE =
    /(?:^|\n\n)\[Transcribed from a voice recording; the original audio is in uploads\/: ([^\n]+)\]\s*$/;

/**
 * What the agent sees: the user's text plus plain-language hints naming the
 * uploaded files. With no recordings the string is identical to the
 * pre-refactor one, so existing sessions and prompt expectations don't shift.
 *
 * `recordings` are dictation clips: the text is their transcript, and the
 * audio sits in `uploads/` without being shown as an attachment
 * (dictation_composer_redesign.md D1). Their hint comes BEFORE the
 * attachments hint, so {@link parseAttachmentHint}'s end-anchored match still
 * finds the attachments one.
 */
export function composeAgentContent(
    text: string,
    names: readonly string[],
    recordings: readonly string[] = [],
): string {
    const parts = [text];
    if (recordings.length > 0) parts.push(`${RECORDING_HINT_PREFIX}${recordings.join(', ')}]`);
    if (names.length > 0) parts.push(`[Attached files in uploads/: ${names.join(', ')}]`);
    return parts.filter(Boolean).join('\n\n');
}

/**
 * The inverse of {@link composeAgentContent} for a message read back from
 * history: the user's text without the hints, the attached file names, and the
 * dictation recordings (which the bubble does not show as chips). Names are
 * split on ", ", so a name that itself contains ", " comes back split
 * (structured attachments are a follow-up, image_generation.md §10).
 */
export function parseAttachmentHint(content: string): {
    text: string;
    names: string[];
    recordings: string[];
} {
    const split = (list: string) => list.split(', ').map((name) => name.trim()).filter(Boolean);
    let text = content;
    let names: string[] = [];
    let recordings: string[] = [];
    const attached = ATTACHMENT_HINT_RE.exec(text);
    if (attached) {
        names = split(attached[1]);
        text = text.slice(0, attached.index);
    }
    const recorded = RECORDING_HINT_RE.exec(text);
    if (recorded) {
        recordings = split(recorded[1]);
        text = text.slice(0, recorded.index);
    }
    return {text, names, recordings};
}

const ATTACHMENT_TYPES: Readonly<Record<string, string>> = {
    png: 'image/png',
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    gif: 'image/gif',
    webp: 'image/webp',
    heic: 'image/heic',
    svg: 'image/svg+xml',
    mp4: 'video/mp4',
    webm: 'video/webm',
    mov: 'video/quicktime',
    mp3: 'audio/mpeg',
    wav: 'audio/wav',
    m4a: 'audio/mp4',
    ogg: 'audio/ogg',
    pdf: 'application/pdf',
};

/** A best guess at an uploaded file's type from its name (for the chip icon). */
export function attachmentTypeFromName(name: string): string {
    const dot = name.lastIndexOf('.');
    const ext = dot > 0 ? name.slice(dot + 1).toLowerCase() : '';
    return ATTACHMENT_TYPES[ext] ?? 'application/octet-stream';
}

/**
 * A permanent refusal from the `none` workspace tier. Matched on the detail
 * text because the endpoint returns 409 for this AND for a pod that is merely
 * still starting (thread_uploads.py:581-588 vs :620-622). The durable fix is a
 * machine-readable {code, message} body like the TTS endpoint already returns
 * (main.py:32570) — until then this string is the only discriminator.
 */
const NO_WORKSPACE_DETAIL = 'no workspace';

/**
 * Terminal failures will never succeed on retry, so the bubble must offer a way
 * out rather than a Retry button that can only fail again:
 *   400 — too many files, or no files provided
 *   413 — a file exceeds the backend's 100MB cap
 *   409 — *only* when the detail says the session has no workspace at all
 * Everything else (409 workspace-not-ready, 502/503 transport, 0 offline) is
 * retryable and keeps the item queued.
 */
export function classifyUploadFailure(status: number, detail = ''): 'terminal' | 'retryable' {
    if (status === 400 || status === 413) return 'terminal';
    if (status === 409 && detail.toLowerCase().includes(NO_WORKSPACE_DETAIL)) return 'terminal';
    return 'retryable';
}

/**
 * Smallest gap between two writes of a file's `loaded` into the outbox signal.
 * 250ms ≈ 4 writes/second.
 *
 * XHR fires UploadProgress far faster than that on a fast link, and every write
 * replaces the outbox array, re-renders the queued bubble and therefore trips
 * the chat view's scroll ResizeObserver — which re-pins to the bottom
 * SYNCHRONOUSLY by design (persistent-chat.component.ts; deferring it into
 * rAF/setTimeout is explicitly forbidden there). Unthrottled, that observer
 * fights a user who scrolls up mid-upload. 4/s is fast enough to read as live
 * and slow enough to leave the wheel/touch scroll-escape in control.
 */
export const PROGRESS_WRITE_INTERVAL_MS = 250;

/**
 * Leading-edge time gate for progress writes: the first event of a burst goes
 * through, the rest wait out the interval. Deliberately has no trailing edge —
 * the terminal state is written by the `done` patch, not by the last progress
 * event, so a dropped tail is invisible.
 */
export function progressWriteDue(
    lastWriteAt: number,
    now: number,
    intervalMs: number = PROGRESS_WRITE_INTERVAL_MS,
): boolean {
    return now - lastWriteAt >= intervalMs;
}

/**
 * Share of the send indicator's scale owned by the byte upload. The remaining
 * 10% belongs to the POST that follows, which is not measurable — so the bar
 * can never reach 100% before the send is accepted, and the accepted send
 * removes the bubble rather than filling the bar. Win32's progress rule: one
 * indicator per operation, never reset between phases.
 */
export const UPLOAD_SHARE_OF_SCALE = 0.9;

function clamp01(x: number): number {
    if (!Number.isFinite(x)) return 0;
    return x < 0 ? 0 : x > 1 ? 1 : x;
}

/**
 * Where one outbox item's send indicator sits, 0-100, or `null` when the
 * fraction is genuinely unknowable and the bar must render indeterminate.
 *
 * Byte-weighted, not file-counted: three files of 90MB, 1MB and 1MB would
 * otherwise jump 33% the moment the two small ones land. `size` is the weight
 * (always known, straight off the File) while `total` — the on-the-wire body
 * length including multipart framing — is the only honest denominator for a
 * partial file. `total` is null whenever the browser cannot compute it, and a
 * file that is actively uploading with no total makes the whole item
 * indeterminate rather than silently contributing 0 or NaN.
 */
export function sendProgressPercent(files: readonly PendingUpload[]): number | null {
    if (files.length === 0) return null;
    let weight = 0;
    let moved = 0;
    for (const f of files) {
        // A 0-byte file still represents one request's worth of work, and a
        // zero total weight would make the division undefined.
        const w = Math.max(f.size, 1);
        weight += w;
        if (f.status === 'done') {
            moved += w;
            continue;
        }
        if (f.status === 'uploading') {
            if (f.total == null || f.total <= 0) return null; // indeterminate
            moved += w * clamp01(f.loaded / f.total);
        }
        // 'queued' and 'failed' have moved nothing that counts.
    }
    return Math.round(clamp01(moved / weight) * UPLOAD_SHARE_OF_SCALE * 100);
}

/** Aggregate state of one outbox item's files, for the bubble's stage line. */
export function uploadSummary(files: readonly PendingUpload[]): {
    done: number;
    total: number;
    allDone: boolean;
    /** Send-indicator position 0-100, or null for indeterminate. */
    percent: number | null;
    firstFailed?: PendingUpload;
} {
    const done = files.filter((f) => f.status === 'done').length;
    return {
        done,
        total: files.length,
        allDone: done === files.length,
        percent: sendProgressPercent(files),
        firstFailed: files.find((f) => f.status === 'failed'),
    };
}
