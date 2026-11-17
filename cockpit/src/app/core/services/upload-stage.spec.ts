import {describe, expect, it} from 'vitest';
import {
    attachmentDedupeKey,
    attachmentTypeFromName,
    classifyUploadFailure,
    composeAgentContent,
    parseAttachmentHint,
    progressWriteDue,
    PROGRESS_WRITE_INTERVAL_MS,
    sendProgressPercent,
    topLevelUploadTargets,
    uploadSummary,
    type PendingUpload,
} from './upload-stage';

function pending(over: Partial<PendingUpload> = {}): PendingUpload {
    return {
        id: 'p1',
        file: new File(['x'], 'a.pdf'),
        name: 'a.pdf',
        size: 1,
        mimeType: 'application/pdf',
        loaded: 0,
        total: 1,
        status: 'queued',
        ...over,
    };
}

describe('composeAgentContent', () => {
    it('appends the uploads hint after the typed text', () => {
        expect(composeAgentContent('look at this', ['a.pdf', 'b.pdf'])).toBe(
            'look at this\n\n[Attached files in uploads/: a.pdf, b.pdf]',
        );
    });

    it('sends the hint alone when there is no typed text', () => {
        expect(composeAgentContent('', ['a.pdf'])).toBe('[Attached files in uploads/: a.pdf]');
    });

    it('returns the text unchanged when there are no files', () => {
        expect(composeAgentContent('hello', [])).toBe('hello');
    });
});

describe('classifyUploadFailure', () => {
    it('treats size and count rejections as terminal', () => {
        expect(classifyUploadFailure(413)).toBe('terminal');
        expect(classifyUploadFailure(400)).toBe('terminal');
    });

    it('treats a not-ready workspace as retryable', () => {
        expect(classifyUploadFailure(409)).toBe('retryable');
        expect(classifyUploadFailure(409, 'Workspace is not ready — try again in a moment')).toBe(
            'retryable',
        );
    });

    it('treats the none-tier refusal as terminal — retrying it can only fail forever', () => {
        expect(
            classifyUploadFailure(409, 'This session has no workspace, so files cannot be attached to it.'),
        ).toBe('terminal');
    });

    it('treats transport failures as retryable', () => {
        expect(classifyUploadFailure(0)).toBe('retryable');
        expect(classifyUploadFailure(502)).toBe('retryable');
        expect(classifyUploadFailure(503)).toBe('retryable');
    });
});

describe('uploadSummary', () => {
    it('counts resolved files and reports not-all-done', () => {
        const s = uploadSummary([
            pending({id: 'a', status: 'done'}),
            pending({id: 'b', status: 'uploading'}),
        ]);
        expect(s).toMatchObject({done: 1, total: 2, allDone: false});
        expect(s.firstFailed).toBeUndefined();
    });

    it('reports allDone when every file resolved', () => {
        const s = uploadSummary([pending({id: 'a', status: 'done'})]);
        expect(s.allDone).toBe(true);
    });

    it('surfaces the first failed file', () => {
        const s = uploadSummary([
            pending({id: 'a', status: 'done'}),
            pending({id: 'b', status: 'failed', error: 'File too large'}),
        ]);
        expect(s.firstFailed?.id).toBe('b');
        expect(s.allDone).toBe(false);
    });

    it('treats an empty list as done', () => {
        expect(uploadSummary([]).allDone).toBe(true);
    });

    it('carries the indicator position alongside the counts', () => {
        const s = uploadSummary([
            pending({id: 'a', size: 100, status: 'done'}),
            pending({id: 'b', size: 100, total: 100, loaded: 50, status: 'uploading'}),
        ]);
        // 150 of 200 bytes, scaled into the 90% the upload owns.
        expect(s.percent).toBe(68);
    });
});

describe('progressWriteDue', () => {
    it('lets the first event of a burst through (leading edge)', () => {
        expect(progressWriteDue(0, 1_000_000)).toBe(true);
    });

    it('drops everything inside the interval', () => {
        const t = 1_000_000;
        expect(progressWriteDue(t, t + PROGRESS_WRITE_INTERVAL_MS - 1)).toBe(false);
    });

    it('reopens exactly at the interval — ~4 writes per second', () => {
        const t = 1_000_000;
        expect(PROGRESS_WRITE_INTERVAL_MS).toBe(250);
        expect(progressWriteDue(t, t + PROGRESS_WRITE_INTERVAL_MS)).toBe(true);
    });
});

describe('sendProgressPercent', () => {
    it('never exceeds the upload share, so the bar cannot fill before the POST', () => {
        // Every byte is in, but the send has not been accepted yet — the
        // remaining scale belongs to the POST.
        expect(sendProgressPercent([pending({status: 'done'})])).toBe(90);
    });

    it('weighs files by size, not by count', () => {
        // A 1MB file landing while a 99MB file has not started is ~1%, not 50%.
        const p = sendProgressPercent([
            pending({id: 'small', size: 1_000_000, status: 'done'}),
            pending({id: 'big', size: 99_000_000, status: 'queued'}),
        ]);
        expect(p).toBe(1);
    });

    it('counts the in-flight file fractionally', () => {
        const p = sendProgressPercent([
            pending({id: 'a', size: 100, total: 100, loaded: 50, status: 'uploading'}),
        ]);
        expect(p).toBe(45); // half the bytes × the 90% upload share
    });

    it('goes indeterminate when the in-flight file has no computable total', () => {
        // HttpUploadProgressEvent.total is optional. Rendering 0% (or NaN)
        // here would be a lie about a file that is actively moving.
        expect(
            sendProgressPercent([
                pending({id: 'a', size: 100, total: null, loaded: 50, status: 'uploading'}),
            ]),
        ).toBeNull();
    });

    it('never divides by zero, and never emits NaN or Infinity', () => {
        expect(
            sendProgressPercent([pending({id: 'a', size: 0, total: 0, loaded: 0, status: 'uploading'})]),
        ).toBeNull();
        const zeroByteDone = sendProgressPercent([
            pending({id: 'a', size: 0, total: null, status: 'done'}),
        ]);
        expect(Number.isFinite(zeroByteDone)).toBe(true);
        expect(zeroByteDone).toBe(90);
    });

    it('clamps a total the browser under-reports (multipart framing) to 100%', () => {
        // `loaded` counts wire bytes; if `total` ever lags them the fraction
        // must not push the bar past its own scale.
        expect(
            sendProgressPercent([
                pending({id: 'a', size: 100, total: 100, loaded: 140, status: 'uploading'}),
            ]),
        ).toBe(90);
    });

    it('reports nothing for an item with no files', () => {
        expect(sendProgressPercent([])).toBeNull();
    });

    it('gives a failed file no credit', () => {
        expect(
            sendProgressPercent([
                pending({id: 'a', size: 100, total: 100, loaded: 90, status: 'failed'}),
            ]),
        ).toBe(0);
    });
});

describe('attachmentDedupeKey', () => {
    it('is Uppy\'s name|size|lastModified', () => {
        expect(attachmentDedupeKey({name: 'a.pdf', size: 12, lastModified: 99})).toBe(
            'a.pdf|12|99',
        );
    });

    it('separates files that differ in any one component', () => {
        const base = {name: 'a.pdf', size: 12, lastModified: 99};
        const keys = new Set([
            attachmentDedupeKey(base),
            attachmentDedupeKey({...base, name: 'b.pdf'}),
            attachmentDedupeKey({...base, size: 13}),
            attachmentDedupeKey({...base, lastModified: 100}),
        ]);
        expect(keys.size).toBe(4);
    });

    it('tolerates a File with no lastModified rather than producing undefined', () => {
        expect(attachmentDedupeKey({name: 'a.pdf', size: 12})).toBe('a.pdf|12|0');
    });
});

describe('topLevelUploadTargets', () => {
    it('names a plain file directly', () => {
        expect(topLevelUploadTargets(['report.pdf'])).toEqual(['report.pdf']);
    });

    it("collapses a zip's members to the one subtree that contains them", () => {
        // The DELETE route removes a named subtree, so a 100-member archive is
        // one request rather than a hundred.
        expect(
            topLevelUploadTargets(['bundle/a.txt', 'bundle/sub/b.txt', 'bundle/sub/c.txt']),
        ).toEqual(['bundle']);
    });

    it('keeps distinct roots distinct', () => {
        expect(topLevelUploadTargets(['bundle/a.txt', 'loose.txt'])).toEqual([
            'bundle',
            'loose.txt',
        ]);
    });

    it('ignores an empty name rather than targeting the uploads root', () => {
        // A DELETE of '' would address uploads/ itself.
        expect(topLevelUploadTargets(['', '/leading.txt'])).toEqual([]);
    });
});


describe('parseAttachmentHint (image_generation WP5: chips survive a reload)', () => {
    it('undoes composeAgentContent', () => {
        const sent = composeAgentContent('Look at these', ['cat.png', 'notes.pdf']);
        expect(parseAttachmentHint(sent)).toEqual({
            text: 'Look at these',
            names: ['cat.png', 'notes.pdf'],
        });
    });

    it('reads a message that is only the hint', () => {
        expect(parseAttachmentHint(composeAgentContent('', ['a.jpg']))).toEqual({
            text: '',
            names: ['a.jpg'],
        });
    });

    it('leaves a message without a trailing hint alone', () => {
        const text = 'I wrote [Attached files in uploads/: x] in the middle\nand more';
        expect(parseAttachmentHint(text)).toEqual({text, names: []});
        expect(parseAttachmentHint('plain')).toEqual({text: 'plain', names: []});
    });

    it('guesses the type from the name for the chip icon', () => {
        expect(attachmentTypeFromName('IMG_0001.JPG')).toBe('image/jpeg');
        expect(attachmentTypeFromName('talk.m4a')).toBe('audio/mp4');
        expect(attachmentTypeFromName('data.bin')).toBe('application/octet-stream');
    });
});
