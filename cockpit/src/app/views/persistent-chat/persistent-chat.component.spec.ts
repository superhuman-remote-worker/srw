import {beforeEach, describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';

import {
    canComposeDuringSession,
    canSendMessage,
    clearDraft,
    composeDenyPrefill,
    countTurnsAfter,
    describeAttachmentRejection,
    draftKey,
    extractClipboardFiles,
    filterRewindCandidates,
    formatPermissionArgs,
    permissionApproveKey,
    permissionTitleKey,
    formatRewindStamp,
    HEADER_FOLD_HYSTERESIS_PX,
    HEADER_LEFT_RESERVE_PX,
    isMicMode,
    isNearBottom,
    formatQueueWait,
    queueWaitTier,
    QUEUE_BUSY_AFTER_MS,
    QUEUE_ELAPSED_AFTER_MS,
    isRewindCommand,
    isStartupBannerVisible,
    loadDraft,
    NEAR_BOTTOM_PX,
    pinTarget,
    pickCodeServerUrlToOpen,
    sshWorkspaceReachable,
    pickCurrentStartupStep,
    pickRewindCandidates,
    pickRunningCommandCard,
    pickWorkspaceOfferCard,
    PersistentChatComponent,
    readingWidthToCss,
    saveDraft,
    shouldFoldHeaderActions,
    shouldFoldToolRun,
    shouldPin,
    shouldSendOnEnter,
    textSizeToCss,
    uploadStageAnnounceKey,
    uploadStageFor,
    uploadStageKey,
} from './persistent-chat.component';
import {cloudReviewBannerVisible} from './cloud-review-banner/cloud-review-banner.component';
import {AssistantTurn, MIN_FOLD_RUN, Turn, UserTurn} from '../../core/models/turn.model';

/**
 * Build a minimal DataTransferItem stand-in. The real DataTransferItemList
 * isn't constructible in jsdom, so we hand-roll the `.kind` / `.getAsFile()`
 * surface the helper reads. A `string` kind returns null from getAsFile, like
 * the browser does.
 */
function clipItem(kind: 'file' | 'string', file: File | null): DataTransferItem {
    return {
        kind,
        type: file?.type ?? 'text/plain',
        getAsFile: () => file,
    } as unknown as DataTransferItem;
}

function itemList(items: DataTransferItem[]): DataTransferItemList {
    // Array.from() in the helper only needs index access + length.
    return items as unknown as DataTransferItemList;
}

/**
 * These cover the three decisions behind the "always-on chat + startup
 * banner" behaviour. The template wiring (banner placement, composer
 * enablement) is exercised by Playwright in the dev-cluster verification —
 * see automations-page.component.spec.ts for the same split.
 */

describe('isStartupBannerVisible', () => {
    it('shows the banner while starting once at least one turn exists', () => {
        expect(isStartupBannerVisible(true, 1)).toBe(true);
        expect(isStartupBannerVisible(true, 5)).toBe(true);
    });

    it('hides the banner on the empty screen — the centered card covers that', () => {
        // Mutually exclusive with the @empty centered startup card, so the
        // card never floats over the message list (the old .resume overlay bug).
        expect(isStartupBannerVisible(true, 0)).toBe(false);
    });

    it('hides the banner once the session is no longer starting', () => {
        expect(isStartupBannerVisible(false, 3)).toBe(false);
        expect(isStartupBannerVisible(false, 0)).toBe(false);
    });
});

describe('queueWaitTier', () => {
    it('is fresh under the 10 s attention limit', () => {
        expect(queueWaitTier(0)).toBe('fresh');
        expect(queueWaitTier(QUEUE_BUSY_AFTER_MS - 1)).toBe('fresh');
    });

    it('escalates to busy at 10 s and long at 60 s', () => {
        expect(queueWaitTier(QUEUE_BUSY_AFTER_MS)).toBe('busy');
        expect(queueWaitTier(QUEUE_ELAPSED_AFTER_MS - 1)).toBe('busy');
        expect(queueWaitTier(QUEUE_ELAPSED_AFTER_MS)).toBe('long');
        expect(queueWaitTier(10 * 60_000)).toBe('long');
    });
});

describe('formatQueueWait', () => {
    it('renders m:ss and clamps negatives', () => {
        expect(formatQueueWait(0)).toBe('0:00');
        expect(formatQueueWait(83_400)).toBe('1:23');
        expect(formatQueueWait(-5)).toBe('0:00');
        expect(formatQueueWait(3_599_000)).toBe('59:59');
    });
});

describe('canComposeDuringSession', () => {
    it('allows composing while the session is starting, even before the SSE opens', () => {
        // Pre-SSE "Creating thread" window: not connected yet, but the user
        // can type and the message is queued + auto-sent on session.state.
        expect(canComposeDuringSession(false, true)).toBe(true);
    });

    it('allows composing once connected', () => {
        expect(canComposeDuringSession(true, false)).toBe(true);
    });

    it('blocks composing when neither connected nor starting (mid-session reconnect)', () => {
        // markSessionReady() only flushes the pending queue once, so we must
        // NOT let a message be queued during a reconnect — it would never send.
        expect(canComposeDuringSession(false, false)).toBe(false);
    });

    it('allows composing in the landing draft, before any thread exists', () => {
        // Instant landing (knowledge-base/knowledge/features/instant_landing_session.md): the
        // first send creates the session; typing must be possible at t=0.
        expect(canComposeDuringSession(false, false, true)).toBe(true);
    });

    it('allows composing on an ended session so a draft can be written first', () => {
        // The box used to go dead the moment a session idled out, stranding a
        // half-typed message the user could read but not edit. Composing is
        // free; only SENDING resumes (and reserves an agent pod + workspace).
        expect(canComposeDuringSession(false, false, false, true)).toBe(true);
    });

    it('keeps the composer open across a send-triggered resume', () => {
        // isStartingSession is false while threadStatus is still 'ended', and
        // the send flips threadStatus before connect() runs — without the
        // isResuming term the box would disable itself mid-send and re-enable
        // a moment later.
        expect(canComposeDuringSession(false, false, false, false, true)).toBe(true);
    });

    it('still blocks a mid-session reconnect once the extra states are false', () => {
        // Guard against the new params quietly turning the reconnect gate off.
        expect(canComposeDuringSession(false, false, false, false, false)).toBe(false);
    });

    it('closes the composer on a live session whose unit is parked', () => {
        // Input into a parked unit is recorded but never claimed until a Retry
        // or an operator unpark revives it: an open box only swallowed it.
        expect(canComposeDuringSession(true, false, false, false, false, true)).toBe(false);
        expect(canComposeDuringSession(false, true, false, false, false, true)).toBe(false);
        expect(canComposeDuringSession(true, false, false, false, false, false)).toBe(true);
    });

    it('leaves an ended or resuming session open despite a parked block', () => {
        // End and suspension settle the unit (the End funnel closes a parked
        // one to done), so the block is stale there and sending still resumes;
        // mid-resume /connection is about to re-derive it.
        expect(canComposeDuringSession(false, false, false, true, false, true)).toBe(true);
        expect(canComposeDuringSession(false, false, false, false, true, true)).toBe(true);
    });
});

describe('canSendMessage', () => {
    // Regression guard: this used to be a computed() over the plain inputText
    // field, so typing never invalidated it and the send button stayed
    // disabled on an idle session (Enter still worked). Now a pure function
    // the component re-evaluates per change-detection pass.
    it('enables send once there is non-whitespace text', () => {
        expect(canSendMessage(true, 'hello', 0)).toBe(true);
    });

    it('stays disabled for empty or whitespace-only text without attachments', () => {
        expect(canSendMessage(true, '', 0)).toBe(false);
        expect(canSendMessage(true, '   \n', 0)).toBe(false);
    });

    it('enables send for attachments alone (voice note, photo)', () => {
        expect(canSendMessage(true, '', 2)).toBe(true);
    });

    it('never sends while composing is blocked, regardless of content', () => {
        expect(canSendMessage(false, 'hello', 1)).toBe(false);
    });
});

describe('uploadStageKey', () => {
    it('reports per-file progress while files remain', () => {
        expect(uploadStageKey({done: 1, total: 3, allDone: false})).toBe('chat.upload.stage');
    });

    it('switches to sending once every file has landed', () => {
        expect(uploadStageKey({done: 3, total: 3, allDone: true})).toBe('chat.upload.sending');
    });

    it('reports nothing for an item with no files', () => {
        expect(uploadStageKey(null)).toBeNull();
    });

    it('takes the percentage variant once the fraction is known', () => {
        expect(uploadStageKey({done: 1, total: 3, allDone: false, percent: 34})).toBe(
            'chat.upload.stagePercent',
        );
    });

    it('falls back to the plain line when the fraction is unknowable', () => {
        // A file uploading with no computable body length. "Uploading 1 of 3 —
        // 0%" would be a lie about bytes that are moving.
        expect(uploadStageKey({done: 1, total: 3, allDone: false, percent: null})).toBe(
            'chat.upload.stage',
        );
    });

    it('still says Sending once the bytes are in, percentage or not', () => {
        // The label changes; the indicator does not. It sits where the upload
        // left it and never fills — the accepted send removes the bubble.
        expect(uploadStageKey({done: 3, total: 3, allDone: true, percent: 90})).toBe(
            'chat.upload.sending',
        );
    });
});

describe('uploadStageAnnounceKey', () => {
    it('strips the percentage for the polite live region', () => {
        // The visible label updates ~4×/s; a live region re-reading
        // "34%… 36%… 39%" is unusable. The announced string changes only when
        // a file lands or the phase flips.
        expect(uploadStageAnnounceKey('chat.upload.stagePercent')).toBe('chat.upload.stage');
    });

    it('leaves every other line alone', () => {
        expect(uploadStageAnnounceKey('chat.upload.sending')).toBe('chat.upload.sending');
        expect(uploadStageAnnounceKey('chat.upload.waitingOn')).toBe('chat.upload.waitingOn');
    });
});

describe('uploadStageFor', () => {
    // Fix round 1: _flushOutbox only ever processes outbox()[0], so a
    // non-head item's own files sit at 'queued' the whole time it waits —
    // showing them would read "Uploading 0 of n…" for work that hasn't
    // started. The honest line names what's actually blocking it: the head's
    // in-flight file.

    it('names the head\'s blocking file for a non-head item that has its own attachments', () => {
        // The item's own summary (2 files, neither started) must be ignored
        // entirely — it is not waiting on itself.
        const ownSummary = {done: 0, total: 2, allDone: false};
        expect(uploadStageFor(false, ownSummary, 'big.mp4')).toEqual({
            key: 'chat.upload.waitingOn',
            params: {name: 'big.mp4'},
        });
    });

    it('names the head\'s blocking file for a non-head item with no attachments of its own', () => {
        // The commonest case: a plain text message queued behind a big
        // upload. No own summary at all, but still not swallowed silently.
        expect(uploadStageFor(false, null, 'big.mp4')).toEqual({
            key: 'chat.upload.waitingOn',
            params: {name: 'big.mp4'},
        });
    });

    it('falls back to no stage line when the head itself has no files to name', () => {
        // A plain text send at the head of the queue — nothing honest to
        // report, so the bubble falls back to its plain queued treatment
        // rather than inventing a filename.
        expect(uploadStageFor(false, null, null)).toBeNull();
    });

    it('leaves the head\'s own upload-progress label unchanged', () => {
        // Regression guard for the new isHead branch: even with a
        // (nonsensical, since the head can't be blocked on itself)
        // headBlockingFileName argument present, the head's own summary
        // alone must drive the label — exactly uploadStageKey's behaviour.
        const ownSummary = {done: 1, total: 3, allDone: false};
        expect(uploadStageFor(true, ownSummary, 'ignored.pdf')).toEqual({
            key: 'chat.upload.stage',
            params: {done: 1, total: 3},
        });
    });

    it('interpolates the percentage into the head\'s label when it is known', () => {
        expect(uploadStageFor(true, {done: 1, total: 3, allDone: false, percent: 34}, null)).toEqual({
            key: 'chat.upload.stagePercent',
            params: {done: 1, total: 3, percent: 34},
        });
    });

    it('omits the percent param entirely when the fraction is unknowable', () => {
        // Not `percent: 0` — a param that is present but meaningless is how a
        // future template starts rendering "0%" for a file that is moving.
        expect(uploadStageFor(true, {done: 1, total: 3, allDone: false, percent: null}, null)).toEqual({
            key: 'chat.upload.stage',
            params: {done: 1, total: 3},
        });
    });

    it('never puts a percentage on a non-head item — one indicator per send', () => {
        // The bytes belong to the head. A percentage here would be a second
        // reading of the same upload on a different bubble.
        expect(
            uploadStageFor(false, {done: 0, total: 2, allDone: false, percent: 12}, 'big.mp4'),
        ).toEqual({key: 'chat.upload.waitingOn', params: {name: 'big.mp4'}});
    });
});

describe('isMicMode', () => {
    it('offers the mic on an empty idle composer with audio input', () => {
        expect(isMicMode(true, false, '', 0)).toBe(true);
    });

    it('flips to send on the first real keystroke', () => {
        expect(isMicMode(true, false, 'h', 0)).toBe(false);
    });

    it('treats whitespace-only text as still empty (nothing sendable)', () => {
        expect(isMicMode(true, false, '  \n', 0)).toBe(true);
    });

    it('flips to send when an attachment is queued without text', () => {
        expect(isMicMode(true, false, '', 1)).toBe(false);
    });

    it('yields to the stop/spinner states while a turn is in flight', () => {
        expect(isMicMode(true, true, '', 0)).toBe(false);
    });

    it('never offers the mic without an audio input device', () => {
        expect(isMicMode(false, false, '', 0)).toBe(false);
    });
});

describe('shouldSendOnEnter', () => {
    it('sends on plain Enter with a physical keyboard', () => {
        expect(shouldSendOnEnter(false, false)).toBe(true);
    });

    it('inserts a newline on Shift+Enter with a physical keyboard', () => {
        expect(shouldSendOnEnter(true, false)).toBe(false);
    });

    it('inserts a newline on Enter on touch devices (send button sends)', () => {
        expect(shouldSendOnEnter(false, true)).toBe(false);
        expect(shouldSendOnEnter(true, true)).toBe(false);
    });
});

describe('pickCurrentStartupStep', () => {
    const steps = [
        {key: 'creating', state: 'done' as const, elapsedMs: 3200},
        {key: 'provisioning', state: 'done' as const, elapsedMs: 6100},
        {key: 'booting', state: 'active' as const, elapsedMs: 17000},
        {key: 'connecting', state: 'todo' as const, elapsedMs: null},
    ];

    it('returns the active step', () => {
        expect(pickCurrentStartupStep(steps)?.key).toBe('booting');
    });

    it('falls back to the last step when none is active', () => {
        const allDone = steps.map((s) => ({...s, state: 'done' as const}));
        expect(pickCurrentStartupStep(allDone)?.key).toBe('connecting');
    });

    it('returns null for an empty list', () => {
        expect(pickCurrentStartupStep([])).toBeNull();
    });
});

describe('pickRunningCommandCard', () => {
    const rt = {id: 'tc1', tool: 'run_command', args: {command: 'sleep 99'}};

    it('returns null when nothing is running', () => {
        expect(pickRunningCommandCard(null, [])).toBeNull();
    });

    it('surfaces the running tool when it is not yet in any turn (cold reattach mid-turn)', () => {
        // Cold reload mid-turn: REST history lacks the in-flight turn, so the
        // only signal is the welcome frame's running_tool — show the card.
        expect(pickRunningCommandCard(rt, [])).toEqual(rt);
    });

    it('suppresses the card when the tool call is already in a visible turn (warm reconnect)', () => {
        // Warm reconnect: the turn (with its own tool card) is still on screen,
        // so the standalone card would double-render — suppress it.
        const turn: AssistantTurn = {
            kind: 'assistant',
            id: 't1',
            status: 'streaming',
            startedAt: 0,
            events: [
                {
                    kind: 'tool_call',
                    id: 'tc1',
                    tool: 'run_command',
                    args: {},
                    status: 'running',
                    startedAt: 0,
                },
            ],
        };
        expect(pickRunningCommandCard(rt, [turn])).toBeNull();
    });
});

/**
 * The batch approval card's per-row argument text. This is the
 * safety-load-bearing piece: "Approve all" runs every call shown, including
 * a destructive shell command, so formatPermissionArgs must never truncate
 * — a truncated arg is a hidden arg. See persistent-chat.component.scss
 * (.permission-args / .permission-list) for how a long result stays fully
 * in the DOM (wrap + scroll) instead of being clipped.
 *
 * What this does NOT cover: that the template's @for actually emits one
 * <li> per pendingPermissions() entry, and that the card's "Approve all" /
 * "Stop" buttons are bound to chat.approveAll() / chat.stop(). The
 * component is never mounted in a spec (see test-setup.ts) — that
 * template-wiring question is checked by code review and by
 * strictTemplates (chat.approveAll / chat.stop must exist and be
 * callable), not by a unit test here. That pendingPermissions() itself
 * carries every entry, not just the first, is proven independently in
 * persistent-chat.service.spec.ts.
 */
/**
 * The approval card is shared by batch turns and ordinary single-gate turns.
 * With one pending call, "Approve all" / "run 1 tool(s)" reads as though
 * something is hidden behind the button — so the copy switches to the
 * singular form. Pure key-pickers so they stay testable: the component is
 * never mounted in a spec (viewChild.required + afterNextRender throw NG0951
 * under this runner).
 */
describe('permissionTitleKey', () => {
    it('uses the singular title for exactly one pending call', () => {
        expect(permissionTitleKey(1)).toBe('chat.permission.singleTitle');
    });

    it('uses the counted batch title for more than one', () => {
        expect(permissionTitleKey(2)).toBe('chat.permission.batchTitle');
        expect(permissionTitleKey(4)).toBe('chat.permission.batchTitle');
    });
});

describe('permissionApproveKey', () => {
    it('says "Approve" for exactly one pending call, not "Approve all"', () => {
        expect(permissionApproveKey(1)).toBe('chat.permission.approve');
    });

    it('says "Approve all" once there is a batch to approve', () => {
        expect(permissionApproveKey(2)).toBe('chat.permission.approveAll');
        expect(permissionApproveKey(4)).toBe('chat.permission.approveAll');
    });
});

describe('formatPermissionArgs', () => {
    it('renders a single string arg as "key: value"', () => {
        expect(formatPermissionArgs({file_path: 'src/app.ts'})).toBe('file_path: src/app.ts');
    });

    it('JSON-stringifies non-string values', () => {
        expect(formatPermissionArgs({count: 3, force: true})).toBe('count: 3, force: true');
    });

    it('joins multiple keys with ", " in insertion order', () => {
        expect(formatPermissionArgs({a: '1', b: '2', c: '3'})).toBe('a: 1, b: 2, c: 3');
    });

    it('returns an empty string when there are no args', () => {
        expect(formatPermissionArgs({})).toBe('');
        expect(formatPermissionArgs(undefined)).toBe('');
        expect(formatPermissionArgs(null)).toBe('');
    });

    it('never truncates — a destructive suffix past 120 chars still reaches the DOM', () => {
        // Regression pin for the CRITICAL finding: an earlier version sliced
        // every value to 120 chars in TypeScript, before the template ever
        // saw it, so "rm -rf /important-data" silently vanished behind an
        // ellipsis with no way to recover it. One click on "Approve all"
        // would have run it, unseen.
        const command = 'echo start && ' + 'a'.repeat(400) + ' && rm -rf /important-data';
        const result = formatPermissionArgs({command});
        expect(result).toContain('rm -rf /important-data');
        expect(result).toBe(`command: ${command}`);
        expect(result).not.toContain('…');
    });

    it('keeps every entry independent across a multi-call batch, not just the first', () => {
        // This is what each row in the @for loop calls; proves it has no
        // shared state and produces correct, distinct text for every
        // pending call in a batch, not only entry 0.
        const perms = [
            {command: 'echo one'},
            {command: 'echo two'},
            {command: 'rm -rf /tmp/scratch'},
        ];
        expect(perms.map((p) => formatPermissionArgs(p))).toEqual([
            'command: echo one',
            'command: echo two',
            'command: rm -rf /tmp/scratch',
        ]);
    });
});

/**
 * approveAndAutoAccept() sets the mode BEFORE approving so a call that
 * immediately follows the just-approved batch (the agent continuing its
 * turn) lands under auto_accept instead of re-prompting. Invoked via
 * .call() against a bare `{chat}` stand-in rather than a constructed
 * component — the class is never mounted in specs (see test-setup.ts),
 * but this method only ever touches `this.chat`, so no other
 * constructor/DI surface is needed to exercise the real, shipped method.
 */
describe('approveAndAutoAccept', () => {
    it('sets auto_accept mode before approving the batch', () => {
        const calls: string[] = [];
        const chat = {
            setMode: vi.fn((mode: string) => calls.push(`setMode:${mode}`)),
            approveAll: vi.fn(() => calls.push('approveAll')),
        };
        const host = {chat} as unknown as PersistentChatComponent;
        PersistentChatComponent.prototype.approveAndAutoAccept.call(host);
        expect(calls).toEqual(['setMode:auto_accept', 'approveAll']);
    });
});

/**
 * F3: the rail (SessionListService) has its own copy of the thread list and
 * only refreshes it on NavigationEnd — a rename is an in-place mutation with
 * no navigation, so nothing else ever tells the rail about it. Same .call()
 * convention as approveAndAutoAccept above: onRenameSession only ever
 * touches this.chat, this.sessionList, this.toast and this.errors.
 */
describe('onRenameSession', () => {
    function makeHost(renameThread: () => Promise<void>) {
        const renameLocal = vi.fn();
        const danger = vi.fn();
        const host = {
            chat: {renameThread},
            sessionList: {renameLocal},
            toast: {danger},
            errors: {translate: (_e: unknown, fallback?: string) => fallback},
        } as unknown as PersistentChatComponent;
        return {host, renameLocal, danger};
    }

    it('patches the rail copy of the thread after a successful rename', async () => {
        const {host, renameLocal} = makeHost(() => Promise.resolve());

        await PersistentChatComponent.prototype.onRenameSession.call(host, 'thread-1', 'New title');

        expect(renameLocal).toHaveBeenCalledWith('thread-1', 'New title');
    });

    it('leaves the rail untouched and toasts when the rename PATCH fails', async () => {
        const {host, renameLocal, danger} = makeHost(() => Promise.reject(new Error('boom')));

        await PersistentChatComponent.prototype.onRenameSession.call(host, 'thread-1', 'New title');

        expect(renameLocal).not.toHaveBeenCalled();
        expect(danger).toHaveBeenCalledWith('errors.sessions.renameFailed');
    });
});

/**
 * R1 follow-up: the header End (disconnectAndLeave) leaves for /sessions only
 * when the End request is finished with. A declined stateless force prompt
 * (`kept`: the session is still running) and a retryable fence (`retryable`:
 * cleanup has not finished; End is the retry) keep the user on the session.
 * Same .call() convention as onRenameSession above.
 */
describe('disconnectAndLeave', () => {
    function makeHost(endSession: () => Promise<unknown>) {
        const isDisconnecting = signal(false);
        const navigate = vi.fn().mockResolvedValue(true);
        const danger = vi.fn();
        const warning = vi.fn();
        const host = {
            isDisconnecting,
            chat: {endSession: vi.fn(endSession)},
            router: {navigate},
            toast: {danger, warning},
            errors: {translate: (_e: unknown, fallback?: string) => fallback},
            transloco: {translate: (key: string) => key},
        } as unknown as PersistentChatComponent;
        return {host, isDisconnecting, navigate, danger, warning};
    }

    it('stays on a stateless session whose End the user declined, with no toast', async () => {
        const {host, isDisconnecting, navigate, danger, warning} = makeHost(() =>
            Promise.resolve('kept'),
        );

        await PersistentChatComponent.prototype.disconnectAndLeave.call(host);

        expect(navigate).not.toHaveBeenCalled();
        expect(danger).not.toHaveBeenCalled();
        expect(warning).not.toHaveBeenCalled();
        expect(isDisconnecting()).toBe(false);
    });

    it('stays and warns when End met a retryable fence, keeping End usable', async () => {
        const {host, isDisconnecting, navigate, danger, warning} = makeHost(() =>
            Promise.resolve('retryable'),
        );

        await PersistentChatComponent.prototype.disconnectAndLeave.call(host);

        expect(warning).toHaveBeenCalledWith('errors.sessions.endRetryable');
        expect(danger).not.toHaveBeenCalled();
        expect(navigate).not.toHaveBeenCalled();
        expect(isDisconnecting()).toBe(false);
    });

    // Guard (holds before and after): a finished End and any other failure
    // keep today's behaviour — leave for /sessions, toasting the failure.
    it('leaves after a finished End, and after any other failure with the danger toast', async () => {
        const done = makeHost(() => Promise.resolve('done'));
        await PersistentChatComponent.prototype.disconnectAndLeave.call(done.host);
        expect(done.navigate).toHaveBeenCalledWith(['/sessions']);
        expect(done.danger).not.toHaveBeenCalled();
        expect(done.warning).not.toHaveBeenCalled();

        const failed = makeHost(() => Promise.reject({status: 500}));
        await PersistentChatComponent.prototype.disconnectAndLeave.call(failed.host);
        expect(failed.danger).toHaveBeenCalledWith('errors.sessions.endFailed');
        expect(failed.warning).not.toHaveBeenCalled();
        expect(failed.navigate).toHaveBeenCalledWith(['/sessions']);
    });
});

describe('pickCodeServerUrlToOpen', () => {
    // The pre-flight-then-open wiring (HTTP through the auth interceptor,
    // window.open, idle-session 401 → re-login) is exercised by Playwright on
    // the dev cluster; here we cover the pure open/skip decision.
    it('returns the code-server URL when the workspace is active', () => {
        expect(
            pickCodeServerUrlToOpen({status: 'active', code_server_url: 'https://api/x/proxy/'}),
        ).toBe('https://api/x/proxy/');
    });

    it('returns null when the status is not active (e.g. restoring)', () => {
        expect(
            pickCodeServerUrlToOpen({status: 'restoring', code_server_url: 'https://api/x/proxy/'}),
        ).toBeNull();
    });

    it('returns null when active but no URL is present', () => {
        expect(pickCodeServerUrlToOpen({status: 'active'})).toBeNull();
    });

    it('returns null when the pre-flight fetch yielded null (swallowed 401 → interceptor re-login)', () => {
        // getThreadIdeStatus swallows a 401 to null *after* the auth interceptor
        // has kicked off re-login; the button must not open a tab in that case.
        expect(pickCodeServerUrlToOpen(null)).toBeNull();
    });
});

describe('sshWorkspaceReachable', () => {
    it('offers SSH wake for a suspended VM and hides unsupported VM access', () => {
        expect(sshWorkspaceReachable({status: 'ready', code_server_url: null}, {state: 'suspended'})).toBe(true);
        expect(sshWorkspaceReachable({status: 'active', code_server_url: 'https://api/x/proxy/'}, {state: 'unsupported'})).toBe(false);
    });
    // The SSH button used to render off capabilities.sshGateway() alone, so it
    // appeared on sessions whose workspace SSH cannot reach (a virtual-tier
    // session answers `srw: this session has no workspace yet`). The gate is
    // the same signal the IDE button uses: does this session have a
    // container-backed workspace right now?
    it('is reachable when the workspace is active', () => {
        expect(sshWorkspaceReachable({status: 'active', code_server_url: 'https://api/x/proxy/'})).toBe(true);
    });

    it('is reachable while the workspace is restoring', () => {
        expect(sshWorkspaceReachable({status: 'restoring', code_server_url: null})).toBe(true);
    });

    it('is reachable when the IDE is withheld but the workspace exists (typed refusal code)', () => {
        // contain_ide_status_for only downgrades payloads that were about to
        // advertise a code-server URL — i.e. the pod is ready. SSH is exactly
        // the working fallback there (ide_credential_key_unconfigured,
        // ide_remote_transport_unavailable).
        expect(
            sshWorkspaceReachable({
                status: 'unavailable',
                code_server_url: null,
                code: 'ide_remote_transport_unavailable',
            }),
        ).toBe(true);
    });

    it('is NOT reachable on a plain unavailable (no container-backed workspace)', () => {
        expect(sshWorkspaceReachable({status: 'unavailable', code_server_url: null})).toBe(false);
    });

    it('is NOT reachable before the first IDE-status poll lands', () => {
        expect(sshWorkspaceReachable(null)).toBe(false);
    });
});

describe('extractClipboardFiles', () => {
    // The (paste) wiring (preventDefault on file pastes, createFilePreviews →
    // addAttachments) is exercised by Playwright on the dev cluster; here we
    // cover the pure clipboard-items → File[] decision.
    const NOW = 1700000000000;

    it('returns [] for a plain-text paste so the default textarea paste runs', () => {
        const items = itemList([clipItem('string', null), clipItem('string', null)]);
        expect(extractClipboardFiles(items, NOW)).toEqual([]);
    });

    it('returns [] for null/undefined clipboard items', () => {
        expect(extractClipboardFiles(null, NOW)).toEqual([]);
        expect(extractClipboardFiles(undefined, NOW)).toEqual([]);
    });

    it('keeps a named file item verbatim', () => {
        const f = new File(['x'], 'report.pdf', {type: 'application/pdf'});
        const out = extractClipboardFiles(itemList([clipItem('file', f)]), NOW);
        expect(out).toHaveLength(1);
        expect(out[0]).toBe(f); // same File instance — no needless re-wrap
        expect(out[0].name).toBe('report.pdf');
    });

    it('synthesizes a stable name + extension for a nameless clipboard blob (screenshot paste)', () => {
        const blob = new File([new Uint8Array([1, 2, 3])], '', {type: 'image/png'});
        const out = extractClipboardFiles(itemList([clipItem('file', blob)]), NOW);
        expect(out).toHaveLength(1);
        expect(out[0].name).toBe(`pasted-${NOW}-0.png`);
        expect(out[0].type).toBe('image/png');
    });

    it('drops the string items and keeps only the file when a paste carries both', () => {
        // Copying an image from a webpage yields an image/png file item AND a
        // text/html string item — we want only the image.
        const img = new File(['x'], 'image.png', {type: 'image/png'});
        const out = extractClipboardFiles(
            itemList([clipItem('string', null), clipItem('file', img)]),
            NOW,
        );
        expect(out).toEqual([img]);
    });

    it('gives each nameless blob a collision-free index in a multi-image paste', () => {
        const a = new File(['a'], '', {type: 'image/png'});
        const b = new File(['b'], '', {type: 'image/jpeg'});
        const out = extractClipboardFiles(itemList([clipItem('file', a), clipItem('file', b)]), NOW);
        expect(out.map((f) => f.name)).toEqual([`pasted-${NOW}-0.png`, `pasted-${NOW}-1.jpeg`]);
    });

    it('falls back to a .bin extension when the blob has no MIME type', () => {
        const blob = new File(['x'], '', {type: ''});
        const out = extractClipboardFiles(itemList([clipItem('file', blob)]), NOW);
        expect(out[0].name).toBe(`pasted-${NOW}-0.bin`);
    });
});

describe('describeAttachmentRejection', () => {
    // The wiring (calling this from onFilesSelected/onPaste/onDrop/the camera
    // path and pushing the result into chat.attachmentError) is exercised by
    // Playwright on the dev cluster; here we cover the pure decision of which
    // key + params to show.

    it('returns null when nothing was rejected', () => {
        expect(describeAttachmentRejection([])).toBeNull();
    });

    it('reports an oversize file with its name interpolated', () => {
        expect(describeAttachmentRejection([{name: 'huge.pdf', reason: 'size'}])).toEqual({
            key: 'chat.upload.tooLarge',
            params: {name: 'huge.pdf'},
        });
    });

    it('reports a count rejection with no filename (generic cap message)', () => {
        expect(describeAttachmentRejection([{name: 'f20.txt', reason: 'count'}])).toEqual({
            key: 'chat.upload.tooManyFiles',
        });
    });

    it('prefers the size reason when a selection trips both at once', () => {
        const rejected = [
            {name: 'a.txt', reason: 'count' as const},
            {name: 'huge.pdf', reason: 'size' as const},
            {name: 'b.txt', reason: 'count' as const},
        ];
        expect(describeAttachmentRejection(rejected)).toEqual({
            key: 'chat.upload.tooLarge',
            params: {name: 'huge.pdf'},
        });
    });
});

describe('shouldFoldToolRun', () => {
    // MIN_FOLD_RUN. Was 4 under the old "runs of consecutive tool calls" rule;
    // now that groupEvents pins the live edge and folds everything else, the only
    // question left is whether a run is long enough to be worth a chip at all —
    // and a 1-event chip is the same height as the card it hides.
    const T = MIN_FOLD_RUN;

    it('folds any run of 2+ when the Tool calls preference is Collapsed (default)', () => {
        expect(shouldFoldToolRun(2, false, T)).toBe(true);
        expect(shouldFoldToolRun(40, false, T)).toBe(true);
    });

    it('leaves a lone event inline — a 1× chip saves nothing', () => {
        expect(shouldFoldToolRun(1, false, T)).toBe(false);
        expect(shouldFoldToolRun(0, false, T)).toBe(false);
    });

    it('never folds when Tool calls is Expanded — the escape hatch still works', () => {
        expect(shouldFoldToolRun(2, true, T)).toBe(false);
        expect(shouldFoldToolRun(50, true, T)).toBe(false);
    });

    it('treats the threshold as inclusive (>=); one below stays inline', () => {
        expect(shouldFoldToolRun(T, false, T)).toBe(true);
        expect(shouldFoldToolRun(T - 1, false, T)).toBe(false);
    });
});

/**
 * Scroll-pin geometry. These are pure on purpose: jsdom has no layout engine, so
 * every geometry read there returns 0 and the pin cannot be tested through the
 * DOM at all — hence the decisions are extracted and tested here.
 *
 * What this does NOT cover: that the ResizeObserver is actually wired to both
 * targets, and that a height change re-pins. The component is never mounted in a
 * spec, and jsdom couldn't answer either question anyway. Those are browser
 * checks — see §"Verification plan" in
 * knowledge-base/knowledge/issues/cockpit_session_scroll_pin_misses_late_height_changes.md.
 */
describe('isNearBottom', () => {
    // scrollHeight 1000, clientHeight 400 → bottom is scrollTop 600.
    it('counts exactly-at-bottom as following', () => {
        expect(isNearBottom(600, 1000, 400)).toBe(true);
    });

    it('counts within the 80px threshold as following', () => {
        expect(isNearBottom(521, 1000, 400)).toBe(true); // 79px away
    });

    it('treats the threshold as exclusive — exactly 80px away is not following', () => {
        expect(isNearBottom(520, 1000, 400)).toBe(false); // 80px away
        expect(isNearBottom(521, 1000, 400)).toBe(true); // 79px away
    });

    it('is not following when scrolled well up', () => {
        expect(isNearBottom(0, 1000, 400)).toBe(false);
    });

    it('counts a list shorter than the viewport as following', () => {
        // Nothing to scroll: scrollHeight <= clientHeight, so we are at the bottom.
        expect(isNearBottom(0, 300, 400)).toBe(true);
    });

    it('honours a caller-supplied threshold', () => {
        expect(isNearBottom(500, 1000, 400, 200)).toBe(true); // 100px away
        expect(isNearBottom(500, 1000, 400, 50)).toBe(false);
    });

    it('defaults to the documented 80px band', () => {
        expect(NEAR_BOTTOM_PX).toBe(80);
    });
});

describe('pinTarget', () => {
    it('returns scrollHeight - clientHeight, NOT scrollHeight', () => {
        // The whole bug: scrollHeight is not a valid scrollTop. Browsers clamp
        // it, so it appears to work — but you write X and read back Y, and
        // onMessagesScroll then recomputes autoScroll from the clamped value.
        expect(pinTarget(1000, 400)).toBe(600);
        expect(pinTarget(1000, 400)).not.toBe(1000);
    });

    it('clamps to 0 when the content is shorter than the viewport', () => {
        // Would otherwise be a negative scrollTop.
        expect(pinTarget(300, 400)).toBe(0);
    });

    it('round-trips: pinning then measuring reports as at-bottom', () => {
        // The invariant that ties the two helpers together — a pin must leave
        // onMessagesScroll computing autoScroll = true, or the pin fights itself.
        const scrollHeight = 1000;
        const clientHeight = 400;
        const top = pinTarget(scrollHeight, clientHeight);
        expect(isNearBottom(top, scrollHeight, clientHeight)).toBe(true);
    });
});

/**
 * Header fold. Pure for the same reason as the scroll pin: jsdom has no layout,
 * so `clientWidth`/`offsetWidth` are all 0 there and the decision cannot be
 * observed through the DOM. What this does NOT cover: that the ResizeObserver
 * is wired to BOTH the header and the action row (observing only the header
 * misses the IDE button appearing), and that the reserve is tuned to the real
 * chrome. Those are browser checks — drag the canvas gutter.
 */
describe('shouldFoldHeaderActions', () => {
    const R = HEADER_LEFT_RESERVE_PX;
    const H = HEADER_FOLD_HYSTERESIS_PX;

    it('keeps the row inline while the title still has its reserve', () => {
        // 1200px header, 420px of buttons → 780 left for a 380 reserve.
        expect(shouldFoldHeaderActions(1200, 420, false)).toBe(false);
    });

    it('folds once the actions would eat into the title reserve', () => {
        // The reported bug: a 2560px desktop hands this header 700px because the
        // canvas owns the rest. Pre-fix the row just ran off the pane edge.
        expect(shouldFoldHeaderActions(700, 420, false)).toBe(true);
    });

    it('treats the reserve as exclusive — exactly filling it still fits', () => {
        expect(shouldFoldHeaderActions(R + 420, 420, false)).toBe(false);
        expect(shouldFoldHeaderActions(R + 420 - 1, 420, false)).toBe(true);
    });

    it('demands slack before unfolding, so a slow drag cannot flip-flop', () => {
        const exactly = R + 420;
        // Unfolding at the same width it folded at is the flicker: fits →
        // unfold → the row reappears → overflows → fold → repeat.
        expect(shouldFoldHeaderActions(exactly, 420, true)).toBe(true);
        expect(shouldFoldHeaderActions(exactly + H, 420, true)).toBe(false);
    });

    it('scales with the action row, not with a device breakpoint', () => {
        // A lite session (no Files/Git/IDE) keeps its buttons far longer than a
        // workspace-backed one at the same pane width — which a media query,
        // and the viewport check this replaced, could never express.
        expect(shouldFoldHeaderActions(700, 160, false)).toBe(false);
        expect(shouldFoldHeaderActions(700, 420, false)).toBe(true);
    });

    it('folds a pane too narrow for the reserve alone, whatever the row costs', () => {
        expect(shouldFoldHeaderActions(300, 0, false)).toBe(true);
    });
});

describe('shouldPin', () => {
    it('pins while the user is following the bottom', () => {
        expect(shouldPin(true, false)).toBe(true);
    });

    it('never pins once the user has scrolled away', () => {
        expect(shouldPin(false, false)).toBe(false);
    });

    it('never pins while restoring position after a prepend', () => {
        // loadOlderHistory parks the viewport mid-list on purpose; a pin there
        // would yank the user to the bottom of the history they just opened.
        expect(shouldPin(true, true)).toBe(false);
        expect(shouldPin(false, true)).toBe(false);
    });
});

describe('readingWidthToCss', () => {
    it('maps comfortable → the 700px reading column', () => {
        expect(readingWidthToCss('comfortable')).toBe('700px');
    });

    it('maps wide → 900px', () => {
        expect(readingWidthToCss('wide')).toBe('900px');
    });

    it('maps full → none (no cap = full-bleed)', () => {
        expect(readingWidthToCss('full')).toBe('none');
    });
});

describe('textSizeToCss', () => {
    it('maps small → 13px', () => {
        expect(textSizeToCss('small')).toBe('13px');
    });

    it('maps medium → the 15px default', () => {
        expect(textSizeToCss('medium')).toBe('15px');
    });

    it('maps large → 17px', () => {
        expect(textSizeToCss('large')).toBe('17px');
    });
});

describe('cloudReviewBannerVisible', () => {
    // Protected cloud mode: the pending-review call to action. Note what is
    // NOT in the predicate — connection state. The review API serves ended
    // threads, so gating this on isConnected() (as the old status-bar badge
    // was) is what hid a genuine pending diff from its owner (PC-25).
    it('shows when the thread is protected and something is staged', () => {
        expect(cloudReviewBannerVisible(true, 3)).toBe(true);
    });

    it('hides when the thread is not protected, even with a nonzero count', () => {
        // Guards against a stale count signal surviving a thread switch away
        // from a protected session.
        expect(cloudReviewBannerVisible(false, 3)).toBe(false);
    });

    it('hides when protected but nothing is staged yet', () => {
        expect(cloudReviewBannerVisible(true, 0)).toBe(false);
    });

    it('hides when neither protected nor staged', () => {
        expect(cloudReviewBannerVisible(false, 0)).toBe(false);
    });
});

/**
 * Composer draft persistence — the un-loseable net for the case where the BFF
 * session genuinely expires and the auth interceptor does a full-page reload.
 * sessionStorage is real under jsdom; clear it between cases.
 */
describe('draft persistence', () => {
    const tid = 'thread-abc';

    beforeEach(() => {
        sessionStorage.clear();
    });

    it('keys a draft under the cockpit:draft: prefix', () => {
        expect(draftKey(tid)).toBe('cockpit:draft:thread-abc');
    });

    it('round-trips a saved draft', () => {
        saveDraft(tid, 'half-written message');
        expect(loadDraft(tid)).toBe('half-written message');
    });

    it('returns empty string for an unknown thread', () => {
        expect(loadDraft('never-saved')).toBe('');
    });

    it('clears a saved draft', () => {
        saveDraft(tid, 'to be cleared');
        clearDraft(tid);
        expect(loadDraft(tid)).toBe('');
    });

    it('isolates drafts by thread id', () => {
        saveDraft('thread-a', 'message A');
        saveDraft('thread-b', 'message B');
        expect(loadDraft('thread-a')).toBe('message A');
        expect(loadDraft('thread-b')).toBe('message B');
    });

    it('removes the key when the text is empty or whitespace', () => {
        saveDraft(tid, 'something');
        saveDraft(tid, '   ');
        expect(loadDraft(tid)).toBe('');
        expect(sessionStorage.getItem(draftKey(tid))).toBeNull();
    });

    it('is a no-op for a null thread id', () => {
        expect(() => saveDraft(null, 'x')).not.toThrow();
        expect(loadDraft(null)).toBe('');
        expect(() => clearDraft(null)).not.toThrow();
    });
});

describe('pickWorkspaceOfferCard', () => {
    const offer = {tier: 'sandbox', reason: 'need to run pytest'};

    it('surfaces a live offer when nothing is provisioning', () => {
        expect(pickWorkspaceOfferCard(offer, null, false)).toEqual({
            state: 'offer',
            tier: 'sandbox',
            reason: 'need to run pytest',
        });
    });

    it('surfaces provisioning when there is no offer', () => {
        expect(pickWorkspaceOfferCard(null, {tier: 'sandbox'}, false)).toEqual({
            state: 'provisioning',
            tier: 'sandbox',
            elapsed: undefined,
            willContinue: false,
        });
    });

    it('lets provisioning win over a still-live offer', () => {
        // The two states must never render at once, whatever the clearing order.
        expect(pickWorkspaceOfferCard(offer, {tier: 'sandbox'}, false)).toMatchObject({
            state: 'provisioning',
        });
    });

    it('returns null when there is neither', () => {
        expect(pickWorkspaceOfferCard(null, null, false)).toBeNull();
    });

    it('passes elapsed through when the tier reports it', () => {
        expect(pickWorkspaceOfferCard(null, {tier: 'vm', elapsed: 120}, false)).toMatchObject({
            elapsed: 120,
        });
    });

    it('leaves elapsed undefined on the sandbox path, which emits no heartbeats', () => {
        expect(pickWorkspaceOfferCard(null, {tier: 'sandbox'}, true)).toMatchObject({
            elapsed: undefined,
        });
    });

    it('passes willContinue through so the two accept buttons differ visually', () => {
        expect(pickWorkspaceOfferCard(null, {tier: 'sandbox'}, true)).toMatchObject({
            willContinue: true,
        });
    });
});

describe('composeDenyPrefill', () => {
    const starter = "Don't upgrade the workspace — ";

    it('prefills the starter into an empty composer', () => {
        expect(composeDenyPrefill('', starter)).toBe(starter);
    });

    it('prefills the starter over whitespace-only text', () => {
        expect(composeDenyPrefill('   \n ', starter)).toBe(starter);
    });

    it('never clobbers what the user already typed', () => {
        expect(composeDenyPrefill('half a thought', starter)).toBe('half a thought');
    });
});

describe('isRewindCommand', () => {
    it('matches the bare command, any casing', () => {
        expect(isRewindCommand('/rewind')).toBe(true);
        expect(isRewindCommand('/REWIND')).toBe(true);
    });

    it('ignores trailing arguments — the picker decides the target', () => {
        expect(isRewindCommand('/rewind to the banana one')).toBe(true);
    });

    it('never matches other commands, prefixed text, or plain chat', () => {
        expect(isRewindCommand('/rewindx')).toBe(false);
        expect(isRewindCommand('/undo')).toBe(false);
        expect(isRewindCommand('rewind')).toBe(false);
        expect(isRewindCommand('please /rewind')).toBe(false);
        expect(isRewindCommand('')).toBe(false);
    });
});

describe('pickRewindCandidates', () => {
    const user = (id: string, historical = true): UserTurn =>
        ({kind: 'user', id, content: `msg ${id}`, timestamp: 0, historical});
    const assistant = (id: string): Turn =>
        ({kind: 'assistant', id, events: [], status: 'complete', startedAt: 0, historical: true} as Turn);

    it('lists only historical user turns, newest first', () => {
        const turns: Turn[] = [user('u1'), assistant('a1'), user('u2'), assistant('a2')];
        expect(pickRewindCandidates(turns, new Set()).map((t) => t.id)).toEqual(['u2', 'u1']);
    });

    it('excludes optimistic bubbles the server has not persisted yet', () => {
        const turns: Turn[] = [user('u1'), user('u2', false)];
        expect(pickRewindCandidates(turns, new Set()).map((t) => t.id)).toEqual(['u1']);
    });

    it('excludes turns still queued in the send outbox', () => {
        const turns: Turn[] = [user('u1'), user('u2')];
        expect(pickRewindCandidates(turns, new Set(['u2'])).map((t) => t.id)).toEqual(['u1']);
    });

    it('returns empty for an empty transcript', () => {
        expect(pickRewindCandidates([], new Set())).toEqual([]);
    });
});

describe('filterRewindCandidates', () => {
    const turn = (id: string, content: string): UserTurn =>
        ({kind: 'user', id, content, timestamp: 0, historical: true});
    const all = [turn('u1', 'Fix the login bug'), turn('u2', 'write STORY.txt'), turn('u3', 'deploy it')];

    it('keeps everything on an empty or whitespace query', () => {
        expect(filterRewindCandidates(all, '')).toEqual(all);
        expect(filterRewindCandidates(all, '   ')).toEqual(all);
    });

    it('matches substrings case-insensitively', () => {
        expect(filterRewindCandidates(all, 'story').map((t) => t.id)).toEqual(['u2']);
        expect(filterRewindCandidates(all, 'THE LOGIN').map((t) => t.id)).toEqual(['u1']);
    });

    it('returns empty when nothing matches', () => {
        expect(filterRewindCandidates(all, 'zebra')).toEqual([]);
    });
});

describe('formatRewindStamp', () => {
    const at = (y: number, mo: number, d: number, h: number, mi: number) =>
        new Date(y, mo, d, h, mi).getTime();

    it('stays time-only for a same-day message', () => {
        expect(formatRewindStamp(at(2026, 7, 8, 19, 8), at(2026, 7, 8, 22, 0), 'en', 'Yesterday')).toBe('19:08');
    });

    it('labels yesterday with the localized word', () => {
        expect(formatRewindStamp(at(2026, 7, 7, 19, 8), at(2026, 7, 8, 22, 0), 'en', 'Yesterday')).toBe('Yesterday 19:08');
    });

    it('handles yesterday across a month boundary', () => {
        expect(formatRewindStamp(at(2026, 6, 31, 9, 0), at(2026, 7, 1, 12, 0), 'en', 'Yesterday')).toBe('Yesterday 09:00');
    });

    it('handles yesterday across a year boundary', () => {
        expect(formatRewindStamp(at(2025, 11, 31, 10, 0), at(2026, 0, 1, 9, 0), 'en', 'Yesterday')).toBe('Yesterday 10:00');
    });

    it('shows a short date without the year within the same year', () => {
        const s = formatRewindStamp(at(2026, 0, 15, 9, 30), at(2026, 7, 8, 12, 0), 'en', 'Yesterday');
        expect(s).toContain('Jan');
        expect(s).toContain('09:30');
        expect(s).not.toContain('2026');
    });

    it('adds the year once it differs', () => {
        const s = formatRewindStamp(at(2025, 10, 3, 14, 5), at(2026, 7, 8, 12, 0), 'en', 'Yesterday');
        expect(s).toContain('2025');
        expect(s).toContain('14:05');
    });
});

describe('countTurnsAfter', () => {
    const user = (id: string): Turn => ({kind: 'user', id, content: '', timestamp: 0, historical: true});
    const assistant = (id: string): Turn =>
        ({kind: 'assistant', id, events: [], status: 'complete', startedAt: 0} as Turn);
    const system = (id: string): Turn => ({kind: 'system', id} as unknown as Turn);

    it('counts user and assistant turns after the target, skipping system lines', () => {
        const turns = [user('u1'), assistant('a1'), system('s1'), user('u2'), assistant('a2')];
        expect(countTurnsAfter(turns, 'u1')).toBe(3);
    });

    it('returns 0 when the target is the last turn', () => {
        expect(countTurnsAfter([user('u1'), user('u2')], 'u2')).toBe(0);
    });

    it('returns -1 when the target is not in the loaded window', () => {
        expect(countTurnsAfter([user('u1')], 'nope')).toBe(-1);
    });
});
