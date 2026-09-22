import {
    afterNextRender,
    AfterViewChecked,
    Component,
    computed,
    DestroyRef,
    effect,
    ElementRef,
    HostListener,
    inject,
    Injector,
    OnDestroy,
    OnInit,
    output,
    QueryList,
    signal,
    viewChild,
    ViewChild,
    ViewChildren,
} from '@angular/core';
import {DatePipe, NgTemplateOutlet, TitleCasePipe} from '@angular/common';
import {HttpClient} from '@angular/common/http';
import {FormsModule} from '@angular/forms';
import {Router} from '@angular/router';
import {firstValueFrom, Subscription} from 'rxjs';
import {MarkdownComponent} from 'ngx-markdown';
import {CitationRefDirective} from '../../core/markdown/citation-ref.directive';
import {WorkspaceFileLinkDirective} from '../../core/markdown/workspace-file-link.directive';
import {KatexDirective} from '../../core/markdown/katex.directive';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ChatAttachment, PermissionRequest, PersistentChatService, RewindPrefill, RunningToolInfo, ToolCallInfo,} from '../../core/services/persistent-chat.service';
import {uploadSummary} from '../../core/services/upload-stage';
import {
    AssistantTurn,
    collapsedAnswer,
    countEvents,
    EventGroup,
    firstSentence,
    firstTextOf,
    foldWakeCycles,
    FoldableEvent,
    FoldedSummary,
    groupEvents,
    isAssistantTurn,
    isSessionBoundary,
    isSystemTurn,
    isUserTurn,
    lastTextOf,
    lastTurnOf,
    MIN_FOLD_RUN,
    nextSpeakingTurn,
    notifyToolCalls,
    summarizeFolded,
    TextEvent,
    ThoughtEvent,
    ToolCallEvent,
    trailingText,
    Turn,
    TurnEvent,
    TurnView,
    UserTurn,
} from '../../core/models/turn.model';
import {ToolCardView} from '../../core/models/tool-card.model';
import {toolCardViewFromEvent} from '../../core/tools/tool-card-adapters';
import {ApiService, IdeSessionStatus} from '../../core/services/api.service';
import {I18nService} from '../../core/services/i18n.service';
import {FileHandlingService} from '../../core/services/file-handling.service';
import {ChatPreferencesService, type ChatTextSize, type ReadingWidth} from '../../core/services/chat-preferences.service';
import {DeviceCapabilitiesService} from '../../core/services/device-capabilities.service';
import {VoiceCapabilitiesService} from '../../core/services/voice-capabilities.service';
import {VoiceRecordingService} from '../../core/services/voice-recording.service';
import {FilePreview, FilePreviewResult, FileType, RejectedFile} from '../../core/models/file.model';
import {RecordingConfig} from '../../core/models/recording.model';
import {environment} from '../../core/environment';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {ViewportService} from '../../core/services/viewport.service';
import {AppButtonComponent} from '../../ui/button';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {AppBadgeComponent} from '../../ui/badge';
import {CitationsPanelComponent} from './citations-panel/citations-panel.component';
import {CloudReviewDialogComponent} from '../job-diff-review/cloud-review-dialog.component';
import {CloudReviewBannerComponent} from './cloud-review-banner/cloud-review-banner.component';
import {SshConnectPanelComponent} from './ssh-connect-panel/ssh-connect-panel.component';
import {ChatEmptyStateComponent, type DisplayedSuggestion} from './chat-empty-state/chat-empty-state.component';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {AppSelectComponent} from '../../ui/select';
import {AppIconComponent} from '../../ui/icon';
import {AppDialogComponent} from '../../ui/dialog';
import {AppToolCardComponent} from '../../ui/tool-card';
import {JobBatchCardComponent} from '../../ui/tool-card/job-batch-card.component';
import {AppReadAloudComponent} from '../../ui/read-aloud';
import {AppInlineEditableTextComponent} from '../../ui/inline-editable-text';
import {AppToastService} from '../../ui/toast';
import {copyText} from '../../ui/copy-field';
import {queueParkReasonKey} from '../../core/models/queue-park-reason';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {ExternalImageDirective} from '../../ui/external-image';
import {SessionListService} from '../../core/services/session-list.service';

interface SlashCommand {
    command: string;
    descriptionKey: string;
}

interface Suggestion {
    icon: string;
    en: string;
    de: string;
}

const SLASH_COMMANDS: SlashCommand[] = [
    {command: '/compact', descriptionKey: 'chat.slash.compact'},
    {command: '/done', descriptionKey: 'chat.slash.done'},
    {command: '/undo', descriptionKey: 'chat.slash.undo'},
    {command: '/rewind', descriptionKey: 'chat.slash.rewind'},
    {command: '/auto', descriptionKey: 'chat.slash.auto'},
    {command: '/supervised', descriptionKey: 'chat.slash.supervised'},
    {command: '/autonomous', descriptionKey: 'chat.slash.autonomous'},
    {command: '/silent', descriptionKey: 'chat.slash.silent'},
    {command: '/verbose', descriptionKey: 'chat.slash.verbose'},
];

const TOOL_LABELS: Record<string, string> = {
    // Workspace — files
    read_file: 'Reading',
    write_file: 'Writing',
    edit_file: 'Editing',
    list_files: 'Exploring files',
    file_exists: 'Checking file',
    delete_file: 'Deleting file',
    search_files: 'Searching files',
    move_file: 'Moving file',
    rename_file: 'Renaming file',
    copy_file: 'Copying file',
    create_directory: 'Creating directory',
    delete_directory: 'Deleting directory',
    get_document_info: 'Inspecting document',

    // Shell
    run_command: 'Running command',
    shell_execute: 'Running command',
    shell_read: 'Reading output',

    // Git
    git_log: 'Viewing git history',
    git_show: 'Viewing commit',
    git_diff: 'Comparing changes',
    git_status: 'Checking git status',
    git_tags: 'Listing tags',
    git_merge_squash: 'Merging changes',
    git_worktree_cleanup: 'Cleaning worktree',

    // Research & web
    web_search: 'Searching the web',
    extract_webpage: 'Extracting page content',
    crawl_website: 'Crawling website',
    map_website: 'Mapping website',
    browse_website: 'Browsing website',
    download_from_website: 'Downloading from web',
    research_topic: 'Researching topic',
    search_papers: 'Searching papers',
    download_paper: 'Downloading paper',
    get_paper_info: 'Getting paper info',

    // Browser (direct)
    browser_navigate: 'Navigating to page',
    browser_snapshot: 'Capturing page',
    browser_click: 'Clicking element',
    browser_type: 'Typing in browser',
    browser_select: 'Selecting option',
    browser_scroll: 'Scrolling page',
    browser_screenshot: 'Taking screenshot',
    browser_back: 'Going back',
    browser_close: 'Closing browser',

    // Tasks & todos
    task_add: 'Adding task',
    task_complete: 'Completing task',
    task_list: 'Listing tasks',
    next_phase_todos: 'Planning next phase',
    todo_complete: 'Completing todo',
    todo_list: 'Listing todos',
    todo_rewind: 'Rewinding todo',

    // Knowledge base
    kb_write: 'Writing to knowledge base',
    kb_update: 'Updating knowledge',
    kb_read: 'Reading knowledge',
    kb_list: 'Listing knowledge',
    kb_search: 'Searching knowledge',
    kb_related: 'Finding related knowledge',

    // Citation
    cite_document: 'Citing document',
    cite_web: 'Citing web source',
    list_sources: 'Listing sources',
    search_library: 'Searching library',

    // Communication & delegation
    send_message: 'Sending message',
    delegate_agent: 'Delegating to subagent',
    wait_agent: 'Waiting for subagent',
    message_agent: 'Messaging subagent',
    stop_agent: 'Stopping subagent',
    list_agents: 'Listing subagents',

    // Job lifecycle
    mark_complete: 'Marking complete',
    job_complete: 'Completing job',
};

/**
 * Terse nouns for the folded-chip count line ("24× searches · 6× thoughts").
 * Deliberately separate from CATEGORY_LABELS below: those are gerund phrases
 * ("Working with files") that read as a heading and don't compose with a
 * leading count. Resolution order matches toolLabel(): i18n key
 * `chat.categoryNouns[One].<key>` first, these maps as fallback.
 *
 * Singular and plural are two flat maps rather than an ICU plural rule because
 * no messageformat plugin is wired into transloco here, and pulling one in for
 * a count line isn't worth a dependency. Both maps must stay key-for-key with
 * each other and with the i18n files.
 */
const CATEGORY_NOUNS_ONE: Record<string, string> = {
    workspace: 'file',
    git: 'git op',
    shell: 'command',
    research: 'search',
    browser_direct: 'browser step',
    core: 'task',
    session_task: 'task',
    knowledge: 'KB op',
    citation: 'citation',
    sql: 'query',
    mongodb: 'query',
    graph: 'graph op',
    cloud: 'cloud op',
    communication: 'message',
    delegation: 'delegation',
    orchestrator: 'fleet op',
    evaluation: 'eval',
    thought: 'thought',
    other: 'step',
};

const CATEGORY_NOUNS: Record<string, string> = {
    workspace: 'files',
    git: 'git ops',
    shell: 'commands',
    research: 'searches',
    browser_direct: 'browser steps',
    core: 'tasks',
    session_task: 'tasks',
    knowledge: 'KB ops',
    citation: 'citations',
    sql: 'queries',
    mongodb: 'queries',
    graph: 'graph ops',
    cloud: 'cloud ops',
    communication: 'messages',
    delegation: 'delegations',
    orchestrator: 'fleet ops',
    evaluation: 'evals',
    // Not a tool category — reasoning gets its own bucket in the count line.
    thought: 'thoughts',
    // Tool calls with no category (older history rows predate the field).
    other: 'steps',
};

/** Categories shown on a chip before the rest roll into "+N more". */
const CHIP_CATEGORY_CAP = 4;

const CATEGORY_LABELS: Record<string, string> = {
    workspace: 'Working with files',
    git: 'Version control',
    shell: 'Running commands',
    research: 'Research & browsing',
    browser_direct: 'Browsing',
    core: 'Task management',
    session_task: 'Task management',
    knowledge: 'Knowledge base',
    citation: 'Citations & sources',
    sql: 'Database queries',
    mongodb: 'Database queries',
    graph: 'Knowledge graph',
    cloud: 'Cloud storage',
    communication: 'Communication',
    delegation: 'Delegating work',
    orchestrator: 'Fleet management',
    evaluation: 'Evaluation',
};

/**
 * Pure decision helpers for the session-startup UX. Exported (and unit
 * tested in persistent-chat.component.spec.ts) so the logic is verified
 * without a full component render — see automations-page.component.spec.ts
 * for the same split.
 */

/**
 * Whether the slim startup banner should show: only once a turn already
 * exists while the session is still starting (a queued message, or a resumed
 * session's history). The @empty centered card covers the no-turns case, so
 * the two are mutually exclusive — the card never floats over the message
 * list (the old .startup-wrapper.resume overlay bug). Hides automatically
 * when the session goes ready (isStartingSession → false).
 */
export function isStartupBannerVisible(isStartingSession: boolean, turnCount: number): boolean {
    return isStartingSession && turnCount > 0;
}

/** Queued-turn bubble escalation: NN/g's 10 s attention limit, then an
 *  elapsed counter from 60 s. Time-based only — never a queue position. */
export const QUEUE_BUSY_AFTER_MS = 10_000;
export const QUEUE_ELAPSED_AFTER_MS = 60_000;
export type QueueWaitTier = 'fresh' | 'busy' | 'long';

export function queueWaitTier(elapsedMs: number): QueueWaitTier {
    if (elapsedMs >= QUEUE_ELAPSED_AFTER_MS) return 'long';
    if (elapsedMs >= QUEUE_BUSY_AFTER_MS) return 'busy';
    return 'fresh';
}

/** m:ss for the queued-turn elapsed counter. */
export function formatQueueWait(elapsedMs: number): string {
    const s = Math.max(0, Math.floor(elapsedMs / 1000));
    return `${Math.floor(s / 60)}:${(s % 60).toString().padStart(2, '0')}`;
}

/**
 * Whether the composer should accept input: while connected OR while the
 * session is still starting (so the user can type from t=0 and have the
 * message queued + auto-sent on session.state). Deliberately false during a
 * mid-session reconnect (connected=false, starting=false) — markSessionReady
 * flushes the pending queue only once, so a message queued then would never
 * send.
 */
/**
 * Map the reading-width preference to the `--chat-content-width` CSS value.
 * `full` → `none` removes the cap (full-bleed); the others are pixel caps.
 */
export function readingWidthToCss(width: ReadingWidth): string {
    switch (width) {
        case 'wide': return '900px';
        case 'full': return 'none';
        default: return '700px';
    }
}

/**
 * Map the text-size preference to the `--chat-body-font-size` CSS value. Only
 * the message body scales; code/tables keep their own fixed sizes.
 */
export function textSizeToCss(size: ChatTextSize): string {
    switch (size) {
        case 'small': return '13px';
        case 'large': return '17px';
        default: return '15px';
    }
}

export function canComposeDuringSession(
    isConnected: boolean,
    isStartingSession: boolean,
    isDraftSession = false,
    isEnded = false,
    isResuming = false,
    isParked = false,
): boolean {
    // A parked unit records new input without reviving it (only an owner
    // Retry or an operator unpark does), so an open box just swallowed the
    // message; the parked bubble carries the way out. Not on an ended or
    // suspended thread: the End funnel settles its unit (a parked one closes
    // to `done`), so a parked block there is stale and sending still resumes;
    // likewise mid-resume, where /connection is about to re-derive it.
    if (isParked && !isEnded && !isResuming) return false;
    // `isEnded` keeps the box live on a resumable session so a user can draft
    // before bringing the agent back. Composing costs nothing; only SENDING
    // resumes (persistent-chat.service.sendMessage), so a half-written message
    // never reserves an agent pod + workspace. `isResuming` covers the window
    // between that send and connect(), which isStartingSession excludes by
    // design (it tests threadStatus !== 'ended').
    return isConnected || isStartingSession || isDraftSession || isEnded || isResuming;
}

export function canSendMessage(canCompose: boolean, text: string, attachmentCount: number): boolean {
    return canCompose && (text.trim().length > 0 || attachmentCount > 0);
}

/**
 * Which label a queued bubble shows for its send stage. One line, one concept:
 * the upload and the POST are phases of the same commitment, so the label
 * changes and the indicator does not. Win32's rule — never reset progress
 * between phases, never reach 100% before the operation completes.
 *
 * `percent` picks the percentage-bearing variant. It is absent (or null)
 * whenever the fraction is unknowable — a file uploading with no computable
 * body length — and then the plain "Uploading 2 of 3…" line is the honest one.
 */
export function uploadStageKey(
    summary: {done: number; total: number; allDone: boolean; percent?: number | null} | null,
): string | null {
    if (!summary || summary.total === 0) return null;
    if (summary.allDone) return 'chat.upload.sending';
    return summary.percent == null ? 'chat.upload.stage' : 'chat.upload.stagePercent';
}

/**
 * The coarse twin of a stage key: what the polite live region says.
 *
 * The visible label updates ~4×/s once a percentage is in it, and a live
 * region that re-reads "34%… 36%… 39%" is unusable. Stripping the percentage
 * makes the announced string change only when a file lands or the phase flips,
 * which is exactly the pace a screen reader wants. The percentage is still
 * reachable on demand via the progressbar's aria-valuetext.
 */
export function uploadStageAnnounceKey(key: string): string {
    return key === 'chat.upload.stagePercent' ? 'chat.upload.stage' : key;
}

/** What the queued bubble's stage line needs to render itself. */
export interface UploadStageView {
    /** Visible label key — may carry the percentage. */
    key: string;
    params: Record<string, string | number>;
    /** Key for the polite live region: the same line minus the percentage. */
    announceKey: string;
    /** Whether THIS item draws the send indicator (only the outbox head does). */
    bar: boolean;
    /** Indicator position 0-100, or null for indeterminate. */
    percent: number | null;
}

/**
 * The full stage-line decision for a queued bubble, covering both queue
 * positions the outbox actually produces — `_flushOutbox` only ever touches
 * `outbox()[0]`, so a non-head item's own files sit untouched at `'queued'`
 * for as long as it waits.
 *
 * The head reports its own progress via `uploadStageKey`, unchanged. A
 * non-head item's own summary is deliberately never consulted: showing its
 * files would read "Uploading 0 of n…" for work that has not started and
 * never lies about being in flight. The honest line names what it is
 * actually blocked on — the head's first not-yet-`done` file — matching
 * `knowledge-base/knowledge/features/session_attachment_send_flow.md`'s "Waiting for
 * Zeugniss.pdf…" example (the Signal #3720 case: a FIFO queue silently
 * swallowing a message typed behind a big upload). If the head has no files
 * of its own (a plain text send ahead of it), there is nothing honest to
 * name, so this falls back to the bubble's plain queued treatment.
 */
export function uploadStageFor(
    isHead: boolean,
    ownSummary: {done: number; total: number; allDone: boolean; percent?: number | null} | null,
    headBlockingFileName: string | null,
): {key: string; params: Record<string, string | number>} | null {
    if (isHead) {
        const key = uploadStageKey(ownSummary);
        if (!key || !ownSummary) return null;
        const params: Record<string, string | number> = {
            done: ownSummary.done,
            total: ownSummary.total,
        };
        // Only when a percentage is actually known — the plain key ignores the
        // param, but shipping a stale one invites a future template to read it.
        if (ownSummary.percent != null) params['percent'] = ownSummary.percent;
        return {key, params};
    }
    return headBlockingFileName ? {key: 'chat.upload.waitingOn', params: {name: headBlockingFileName}} : null;
}

/**
 * Empty-composer morph (messenger convention): the round action button offers
 * dictation while there is nothing to send yet, and flips to send on the
 * first keystroke or queued attachment. Suppressed while a turn is in flight
 * so the stop/spinner states keep the button.
 */
export function isMicMode(
    hasAudioInput: boolean,
    turnInFlight: boolean,
    text: string,
    attachmentCount: number,
): boolean {
    return hasAudioInput && !turnInFlight && text.trim().length === 0 && attachmentCount === 0;
}

/**
 * Enter-key semantics: physical keyboards send on plain Enter (Shift+Enter
 * for a newline); on touch devices Enter always inserts a newline — the
 * virtual-keyboard Enter sits where a thumb expects "new line", and the send
 * button is the send affordance.
 */
export function shouldSendOnEnter(shiftKey: boolean, isMobileDevice: boolean): boolean {
    return !shiftKey && !isMobileDevice;
}

/**
 * The startup step to surface in the banner: the active one, or the last
 * step as a fallback when none is active (the brief gap where every phase is
 * recorded done before sessionReady flips). Null for an empty list.
 */
export function pickCurrentStartupStep<T extends {state: string}>(steps: readonly T[]): T | null {
    return steps.find((s) => s.state === 'active') ?? steps[steps.length - 1] ?? null;
}

/**
 * The running-command card to surface on (re)attach, or null. Suppressed when
 * the in-flight tool call is already rendered inside a visible turn (warm
 * reconnect) so it isn't double-shown; surfaced when it isn't (cold reload
 * mid-turn, where the in-flight turn isn't in REST history yet).
 */
export function pickRunningCommandCard(
    runningTool: RunningToolInfo | null,
    turns: readonly Turn[],
): RunningToolInfo | null {
    if (!runningTool) return null;
    for (const turn of turns) {
        if (!isAssistantTurn(turn)) continue;
        for (const ev of turn.events) {
            if (ev.kind === 'tool_call' && ev.id === runningTool.id) return null;
        }
    }
    return runningTool;
}

/**
 * Full argument content for one pending permission call — every arg, in
 * full, joined as "key: value, key: value". This is the entire safety model
 * for the batch approval card: "Approve all" runs every call shown,
 * including a destructive shell command, so nothing here may ever be
 * truncated. A long value wraps and the row list scrolls instead
 * (.permission-args / .permission-list in persistent-chat.component.scss)
 * — it must never be clipped out of the DOM.
 */
export function formatPermissionArgs(args: Record<string, unknown> | null | undefined): string {
    const safe = args ?? {};
    return Object.entries(safe)
        .map(([k, v]) => `${k}: ${typeof v === 'string' ? v : JSON.stringify(v)}`)
        .join(', ');
}

/** Title key for the approval card: singular reads oddly as "1 tool(s)". */
export function permissionTitleKey(count: number): string {
    return count === 1 ? 'chat.permission.singleTitle' : 'chat.permission.batchTitle';
}

/** Approve-button key. "Approve all" on a lone call implies hidden extras. */
export function permissionApproveKey(count: number): string {
    return count === 1 ? 'chat.permission.approve' : 'chat.permission.approveAll';
}

/** The workspace-upgrade card to render, or null when there's nothing to show. */
export type WorkspaceOfferCard =
    | {state: 'provisioning'; tier: string; elapsed?: number; willContinue: boolean}
    | {state: 'offer'; tier: string; reason: string}
    | null;

/**
 * Pick the workspace-upgrade card state. Provisioning strictly wins over a live
 * offer so the two can never render at once — upgradeWorkspace() clears the
 * offer synchronously today, but the invariant shouldn't depend on that
 * ordering holding.
 */
export function pickWorkspaceOfferCard(
    offer: {tier: string; reason: string} | null,
    inProgress: {tier: string; elapsed?: number} | null,
    willContinue: boolean,
): WorkspaceOfferCard {
    if (inProgress) {
        return {
            state: 'provisioning',
            tier: inProgress.tier,
            elapsed: inProgress.elapsed,
            willContinue,
        };
    }
    if (offer) return {state: 'offer', tier: offer.tier, reason: offer.reason};
    return null;
}

/**
 * The composer text to show when declining an upgrade offer. Never clobbers
 * what the user has already typed — unlike pickSuggestion, which assigns
 * unconditionally because it only ever fires against an empty landing composer.
 */
export function composeDenyPrefill(existingText: string, starter: string): string {
    return existingText.trim().length > 0 ? existingText : starter;
}

/** True when the composer text is the /rewind command (any casing, with or
 *  without trailing arguments — arguments are ignored, the picker decides). */
export function isRewindCommand(text: string): boolean {
    return text.toLowerCase().split(/\s+/)[0] === '/rewind';
}

/**
 * User turns eligible as rewind targets, newest first — the /rewind picker's
 * list. Same gate as the inline hover button: only historical turns (already
 * persisted server-side) that are not still queued in the send outbox can
 * anchor a rewind.
 */
export function pickRewindCandidates(turns: readonly Turn[], outboxIds: ReadonlySet<string>): UserTurn[] {
    const users: UserTurn[] = [];
    for (const turn of turns) {
        if (turn.kind === 'user' && turn.historical && !outboxIds.has(turn.id)) {
            users.push(turn);
        }
    }
    return users.reverse();
}

/** Show the picker's filter input only when the list is long enough that
 *  scanning beats scrolling; short sessions keep the plain hint line. */
export const REWIND_FILTER_MIN_CANDIDATES = 6;

/** Case-insensitive substring filter over picker candidates. An empty or
 *  whitespace query keeps everything. */
export function filterRewindCandidates(candidates: readonly UserTurn[], query: string): UserTurn[] {
    const q = query.trim().toLowerCase();
    if (!q) return [...candidates];
    return candidates.filter((t) => t.content.toLowerCase().includes(q));
}

/**
 * Date-aware timestamp for the rewind surfaces. Sessions span days, so a bare
 * clock time ("19:08") is ambiguous there: today stays time-only, yesterday is
 * labeled (caller passes the localized word), older dates get a short
 * localized date — with the year only once it differs.
 */
export function formatRewindStamp(ts: number, now: number, locale: string, yesterdayLabel: string): string {
    const d = new Date(ts);
    const n = new Date(now);
    const hm = `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
    const sameDay = (a: Date, b: Date) =>
        a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
    if (sameDay(d, n)) return hm;
    const yesterday = new Date(n);
    yesterday.setDate(n.getDate() - 1);
    if (sameDay(d, yesterday)) return `${yesterdayLabel} ${hm}`;
    const opts: Intl.DateTimeFormatOptions = {month: 'short', day: 'numeric'};
    if (d.getFullYear() !== n.getFullYear()) opts.year = 'numeric';
    return `${new Intl.DateTimeFormat(locale || undefined, opts).format(d)}, ${hm}`;
}

/**
 * How many visible messages (user + assistant turns; system lines and
 * compaction markers don't read as "messages") come after the target — what a
 * conversation rewind hides besides returning the target prompt itself to the
 * composer. -1 when the target isn't in the loaded window.
 */
export function countTurnsAfter(turns: readonly Turn[], targetId: string): number {
    const at = turns.findIndex((t) => t.id === targetId);
    if (at === -1) return -1;
    let count = 0;
    for (let i = at + 1; i < turns.length; i++) {
        const kind = turns[i].kind;
        if (kind === 'user' || kind === 'assistant') count++;
    }
    return count;
}

/**
 * Decide whether the IDE button should open code-server, and to which URL.
 * Returns the code-server URL only when the workspace is active; null
 * otherwise — including when the pre-flight status fetch returned null (a
 * swallowed 401 the auth interceptor has already turned into a re-login).
 */
export function pickCodeServerUrlToOpen(status: IdeSessionStatus | null): string | null {
    return status?.status === 'active' && status.code_server_url
        ? status.code_server_url
        : null;
}

/**
 * Whether this session's workspace is something the SSH gateway can reach.
 * The SSH button used to render off `capabilities.sshGateway()` alone and so
 * appeared on sessions with no container-backed workspace at all (connecting
 * answers `srw: this session has no workspace yet`). Reachability is the same
 * signal the IDE button reads: `active` and `restoring` mean a pod exists or
 * is being made, and a typed refusal `code` means the IDE advertisement was
 * withheld from a pod that IS ready (contain_ide_status_for only downgrades
 * payloads that were about to advertise a URL) — where SSH is exactly the
 * working fallback.
 */
export function sshWorkspaceReachable(status: IdeSessionStatus | null): boolean {
    if (!status) return false;
    return status.status === 'active' || status.status === 'restoring' || !!status.code;
}

/**
 * Pull file payloads out of a paste's clipboard items (#11 paste-to-attach),
 * dropping the `kind: 'string'` entries (plain text / HTML) so a text paste
 * falls through to the textarea untouched. Clipboard images frequently arrive
 * as a nameless blob — synthesize a stable, collision-free filename so the
 * attachment chip has a label and the agent hint reads sensibly. Pure and
 * DOM-creation-free (takes the item list, takes `now` instead of calling
 * Date.now()) so the selection logic is unit-testable; the component handler
 * does the async preview + signal update.
 */
export function extractClipboardFiles(
    items: DataTransferItemList | null | undefined,
    now: number,
): File[] {
    if (!items) return [];
    const files: File[] = [];
    for (const item of Array.from(items)) {
        if (item.kind !== 'file') continue;
        const file = item.getAsFile();
        if (!file) continue;
        if (file.name) {
            files.push(file);
        } else {
            const ext = (file.type.split('/')[1] || 'bin').split(';')[0];
            files.push(
                new File([file], `pasted-${now}-${files.length}.${ext}`, {
                    type: file.type,
                    lastModified: now,
                }),
            );
        }
    }
    return files;
}

/**
 * Turn a createFilePreviews() rejection list into a single translation key +
 * params for the composer's `chat.attachmentError` banner. Only one message
 * can show at a time, so when a selection trips both reasons at once the
 * size rejection wins: a file that silently exceeded the byte cap is more
 * surprising than one dropped for the (generous, 20-file) composer count
 * cap, and the count message doesn't need a filename anyway. Pure so it's
 * unit-testable without mounting PersistentChatComponent (NG0951).
 */
export function describeAttachmentRejection(
    rejected: readonly RejectedFile[],
): {key: string; params?: Record<string, unknown>} | null {
    if (rejected.length === 0) return null;
    const oversized = rejected.find((r) => r.reason === 'size');
    return oversized
        ? {key: 'chat.upload.tooLarge', params: {name: oversized.name}}
        : {key: 'chat.upload.tooManyFiles'};
}

/**
 * Whether a run of consecutive tool calls should render as the folded
 * "N× tool calls" disclosure vs. plain inline cards. A run folds only when the
 * user hasn't chosen the always-inline "Tool calls → Expanded" preference AND
 * it's long enough to be worth grouping (Slice 3 threshold). When expanded, no
 * run folds — every call renders inline with no fold control, like a short run.
 */
export function shouldFoldToolRun(
    toolCount: number,
    toolCallsExpanded: boolean,
    threshold: number,
): boolean {
    return !toolCallsExpanded && toolCount >= threshold;
}

/**
 * Scroll-pin geometry. Extracted as pure functions because jsdom has no layout
 * engine — every geometry read there returns 0 — so the *decisions* are unit
 * tested here and the *wiring* is verified in a real browser. See
 * knowledge-base/knowledge/issues/cockpit_session_scroll_pin_misses_late_height_changes.md
 * §"Verification plan".
 */

/** How close to the bottom still counts as "following". */
export const NEAR_BOTTOM_PX = 80;

/**
 * Whether the viewport is close enough to the bottom to count as following it.
 * 80px sits in the practitioner band (use-stick-to-bottom 70, Vercel 100,
 * Element 200); it is the long-standing value here and is kept deliberately.
 */
export function isNearBottom(
    scrollTop: number,
    scrollHeight: number,
    clientHeight: number,
    threshold: number = NEAR_BOTTOM_PX,
): boolean {
    return scrollHeight - scrollTop - clientHeight < threshold;
}

/**
 * The scrollTop that actually parks the viewport at the bottom.
 *
 * NOT `scrollHeight` — that is not a valid scrollTop. Browsers clamp it, so it
 * looks like it works, but you write X and read back Y, and `onMessagesScroll`
 * then recomputes `autoScroll` from the clamped value. Clamped to 0 so a
 * shorter-than-viewport list yields a real coordinate rather than a negative.
 */
export function pinTarget(scrollHeight: number, clientHeight: number): number {
    return Math.max(0, scrollHeight - clientHeight);
}

/**
 * Whether a height change should re-pin the viewport to the bottom. Both guards
 * matter: `autoScroll` is the user's follow intent, and `isRestoringScroll`
 * covers the prepend path, which deliberately parks the viewport mid-list.
 */
export function shouldPin(autoScroll: boolean, isRestoringScroll: boolean): boolean {
    return autoScroll && !isRestoringScroll;
}

/**
 * Header-fold geometry. Same reason as the scroll pin above: the decision is
 * pure and unit tested, the wiring is a browser check.
 *
 * Inline space the left group keeps before the actions fold: its fixed chrome
 * (sidebar toggle, back, icon, session id, status dot + label ≈ 260px) plus a
 * still-readable slice of the session title (≈ 120px). This is the only knob,
 * and it means "fold once the title would be squeezed below ~120px" — NOT a
 * pane breakpoint. The right value scales with the header's chrome, not with
 * any device width.
 */
export const HEADER_LEFT_RESERVE_PX = 380;

/**
 * Slack required to unfold again. Without it the two states sit one pixel apart
 * and a slow gutter drag flickers the header between them.
 */
export const HEADER_FOLD_HYSTERESIS_PX = 24;

/**
 * Whether the header's action row should collapse into the `⋮` overflow menu.
 *
 * `folded` is the CURRENT state and it is not decoration: folding shrinks the
 * row to ~150px, which would otherwise "prove" there is room, unfold, overflow,
 * and fold again. So the caller passes the row's natural (unfolded) width — the
 * last one it measured while unfolded — and unfolding additionally demands
 * `hysteresis` px of slack.
 */
export function shouldFoldHeaderActions(
    headerInnerWidth: number,
    actionsNaturalWidth: number,
    folded: boolean,
    reserve: number = HEADER_LEFT_RESERVE_PX,
    hysteresis: number = HEADER_FOLD_HYSTERESIS_PX,
): boolean {
    const room = headerInnerWidth - reserve;
    return actionsNaturalWidth + (folded ? hysteresis : 0) > room;
}


/**
 * Composer draft persistence — survive a full-page reload (e.g. the auth
 * redirect that fires when a BFF session genuinely expires) without losing an
 * unsent message. Keyed by thread id in sessionStorage: per-tab, survives the
 * same-tab OIDC round-trip + reload, and auto-clears when the tab closes.
 * Writes are synchronous (no debounce) so the latest text is always persisted
 * before any abrupt navigation. Every call is best-effort — storage can throw
 * (private mode / quota) and a lost draft must never break the composer.
 */
const DRAFT_KEY_PREFIX = 'cockpit:draft:';

export function draftKey(threadId: string): string {
    return `${DRAFT_KEY_PREFIX}${threadId}`;
}

export function saveDraft(threadId: string | null, text: string): void {
    if (!threadId) return;
    try {
        if (text && text.trim()) {
            sessionStorage.setItem(draftKey(threadId), text);
        } else {
            sessionStorage.removeItem(draftKey(threadId));
        }
    } catch {
        /* storage unavailable / quota — drafts are best-effort */
    }
}

export function loadDraft(threadId: string | null): string {
    if (!threadId) return '';
    try {
        return sessionStorage.getItem(draftKey(threadId)) ?? '';
    } catch {
        return '';
    }
}

export function clearDraft(threadId: string | null): void {
    if (!threadId) return;
    try {
        sessionStorage.removeItem(draftKey(threadId));
    } catch {
        /* noop */
    }
}


@Component({
    selector: 'app-persistent-chat',
    standalone: true,
    imports: [
        FormsModule,
        NgTemplateOutlet,
        TitleCasePipe,
        DatePipe,
        MarkdownComponent,
        ExternalImageDirective,
        CitationRefDirective,
        WorkspaceFileLinkDirective,
        KatexDirective,
        SidebarToggleComponent,
        TranslocoPipe,
        AppButtonComponent,
        AppIconButtonComponent,
        AppMenuComponent,
        AppMenuItemComponent,
        AppMenuTriggerDirective,
        AppBadgeComponent,
        AppSelectComponent,
        AppIconComponent,
        AppDialogComponent,
        AppToolCardComponent,
        JobBatchCardComponent,
        AppReadAloudComponent,
        AppInlineEditableTextComponent,
        CitationsPanelComponent,
        CloudReviewDialogComponent,
        CloudReviewBannerComponent,
        SshConnectPanelComponent,
        ChatEmptyStateComponent,
    ],
    template: `
    <div class="chat-container"
         [style.--chat-content-width]="chatWidthValue()"
         [style.--chat-body-font-size]="chatTextSizeValue()">
      <!-- Drag-and-drop overlay (covers the chat area while files are being dragged) -->
      @if (isDragOver()) {
        <div class="drop-overlay" aria-hidden="true">
          <div class="drop-overlay-card">
            <app-icon size="lg" class="drop-overlay-icon">cloud_upload</app-icon>
            <span class="drop-overlay-text">{{ 'chat.composer.dropHint' | transloco }}</span>
          </div>
        </div>
      }

      <!-- Header -->
      <div class="chat-header" #chatHeaderEl>
        <div class="header-left">
          <app-sidebar-toggle />
          <span class="header-title">
            @if (chat.threadId(); as tid) {
              <app-inline-editable-text
                [value]="chat.sessionTitle() || ('chat.defaultTitle' | transloco)"
                [clickToEdit]="true"
                [ariaLabel]="'common.rename' | transloco"
                (save)="onRenameSession(tid, $event)"
              />
            } @else {
              {{ chat.sessionTitle() || ('chat.defaultTitle' | transloco) }}
            }
          </span>
          <span class="status-dot" [class]="connectionClass()" [title]="connectionLabel()"></span>
          <span class="status-announce">{{ connectionLabel() }}</span>

          @if (chat.isConnected()) {
            <!-- Danger first, deliberately: app-badge is flex-shrink:0 +
                 white-space:nowrap and .header-left has no overflow
                 fallback, so under width pressure the title ellipsizes to
                 nothing and then the LAST badges clip silently. A degraded
                 cloud sync (the user's files may not be saving) must never
                 be the one that clips. -->
            @if (chat.cloudSyncDegraded()) {
              <app-badge tone="danger" size="sm" [title]="'chat.status.cloudSyncOffTooltip' | transloco">
                {{ 'chat.status.cloudSyncOff' | transloco }}
              </app-badge>
            }
            @if (chat.modelName()) {
              <app-badge tone="accent" size="sm" role="button" tabindex="0"
                         [title]="'chat.header.settingsTooltip' | transloco"
                         (click)="settingsRequested.emit('model')"
                         (keydown.enter)="settingsRequested.emit('model')">{{ chat.modelName() }}</app-badge>
            }
            <app-badge tone="accent" size="sm" role="button" tabindex="0"
                       [title]="'chat.header.settingsTooltip' | transloco"
                       (click)="settingsRequested.emit(undefined)"
                       (keydown.enter)="settingsRequested.emit(undefined)">{{ chat.permissionMode() | titlecase }}</app-badge>

            <!-- Transient conditions: conditional and usually absent, so they cost
                 nothing when nothing is wrong. -->
            @if (chat.agentSilenceSeconds() >= 30 && !chat.compaction()) {
              <app-badge tone="warning" size="sm">{{ 'chat.status.agentQuiet' | transloco:{ seconds: chat.agentSilenceSeconds() } }}</app-badge>
            }
            @if (chat.compaction(); as comp) {
              <app-badge tone="warning" size="sm">{{ 'chat.compactionLive.footer' | transloco:{ current: comp.currentPass > 0 ? comp.currentPass : 1, total: comp.nPasses, elapsed: compactionElapsed() } }}</app-badge>
            }
          }
        </div>
        <div class="header-right" #headerActionsEl>
          <ng-content select="[chatHeaderAction]" />
          @if (chat.isConnected()) {
            <!-- Secondary controls always fold into one overflow menu now
                 (used to be conditional on headerCompact(), which survives
                 for the ended-session buttons below). Disconnect stays its
                 own visible button, not folded in. -->
            <app-icon-button
              size="sm"
              [ariaLabel]="'chat.header.moreActions' | transloco"
              [appMenuTrigger]="headerMenu"
              menuPlacement="bottom-end"
            >
              <app-icon size="sm">more_vert</app-icon>
            </app-icon-button>
            <app-menu #headerMenu>
              @if (chat.threadId(); as tid) {
                <app-menu-item (activated)="copyThreadId()">{{ 'chat.header.copySessionId' | transloco:{ id: tid.slice(0, 8) } }}</app-menu-item>
              }
              <app-menu-item (activated)="settingsRequested.emit(undefined)">{{ 'chat.header.settingsTooltip' | transloco }}</app-menu-item>
              <app-menu-item (activated)="showViewMenu.update(v => !v)">{{ 'chat.header.viewMenuTooltip' | transloco }}</app-menu-item>
              @if (chat.citationsByCid().size > 0) {
                <app-menu-item (activated)="showCitations.update(v => !v)">{{ 'chat.header.citationsButton' | transloco }}</app-menu-item>
              }
              @if (chat.verifiedProjectFolder()) {
                <app-menu-item (activated)="openProjectFiles()">{{ 'chat.header.projectFilesButton' | transloco }}</app-menu-item>
              }
              @if (chat.cloudSessionUrl() || chat.ncSessionFolder()) {
                <app-menu-item (activated)="openSessionFiles()">{{ sessionFilesLabelKey() | transloco }}</app-menu-item>
              }
              @if (ideStatus()?.gitea_url; as giteaUrl) {
                <app-menu-item (activated)="openIde(giteaUrl)">{{ 'chat.header.gitButton' | transloco }}</app-menu-item>
              }
              @if (ideStatus()?.status === 'active' && ideStatus()?.code_server_url) {
                <app-menu-item (activated)="openCodeServer()">{{ 'chat.header.ideButton' | transloco }}</app-menu-item>
              }
              @if (ideStatus()?.status === 'restoring') {
                <app-menu-item [disabled]="true">{{ 'chat.header.ideLoadingTooltip' | transloco }}</app-menu-item>
              }
              @if (sshButtonVisible()) {
                <app-menu-item (activated)="showSshPanel.update(v => !v)">{{ 'chat.header.sshButton' | transloco }}</app-menu-item>
              }
            </app-menu>
            <app-button variant="ghost" size="sm"
                        [loading]="isDisconnecting()"
                        [ariaLabel]="(isDisconnecting() ? 'chat.header.disconnecting' : 'chat.header.disconnect') | transloco"
                        (clicked)="disconnectAndLeave()">
              {{ 'chat.header.disconnect' | transloco }}
            </app-button>
          } @else if (chat.cloudSessionUrl() || chat.ncSessionFolder() || chat.verifiedProjectFolder()) {
            <!-- Asleep/ended session. Every other header action drives the live
                 agent, so the isConnected() gate above is right for them — but
                 these just open an external cloud URL that loadThreadMeta has
                 already resolved, and they are the one deliverable surface a
                 user comes back to a dead session for. Keep them reachable.

                 PC-19: for a protected session the project folder is the one
                 the staged diff applies to; the legacy session folder is an
                 empty agent-service directory. Both are offered, each named
                 for what it is — and the project action exists only when the
                 mount has been cross-checked against the diff summary. -->
            @if (chat.verifiedProjectFolder(); as folder) {
              @if (headerCompact()) {
                <app-icon-button
                  size="sm"
                  [ariaLabel]="'chat.header.projectFilesButton' | transloco"
                  [tooltip]="'chat.header.projectFilesTooltip' | transloco:{ name: folder.name }"
                  (clicked)="openProjectFiles()"
                >
                  <app-icon size="sm">folder_shared</app-icon>
                </app-icon-button>
              } @else {
                <button class="ide-btn" (click)="openProjectFiles()"
                        [title]="'chat.header.projectFilesTooltip' | transloco:{ name: folder.name }">
                  <app-icon size="sm" class="ide-icon">folder_shared</app-icon>
                  {{ 'chat.header.projectFilesButton' | transloco }}
                </button>
              }
            }
            @if (chat.cloudSessionUrl() || chat.ncSessionFolder()) {
              @if (headerCompact()) {
                <app-icon-button
                  size="sm"
                  [ariaLabel]="sessionFilesLabelKey() | transloco"
                  [tooltip]="'chat.header.filesTooltip' | transloco"
                  (clicked)="openSessionFiles()"
                >
                  <app-icon size="sm">cloud</app-icon>
                </app-icon-button>
              } @else {
                <button class="ide-btn" (click)="openSessionFiles()" [title]="'chat.header.filesTooltip' | transloco">
                  <app-icon size="sm" class="ide-icon">cloud</app-icon>
                  {{ sessionFilesLabelKey() | transloco }}
                </button>
              }
            }
          }
        </div>
      </div>

      <!-- Pending protected-cloud review. Deliberately OUTSIDE the
           isConnected() gate above: the review API serves ended threads on
           purpose, and gating the only entry point on the live agent turned a
           recoverable duplicate stage into a trap whose only exit was Resume
           (PC-25). Its own component so no rules land in
           persistent-chat.component.scss, which is already 39.38kB of a 48kB
           anyComponentStyle error budget. -->
      <app-cloud-review-banner
        [protectedCloud]="chat.protectedCloud()"
        [count]="chat.cloudChangesCount()"
        [probe]="chat.cloudDiffProbe()"
        [folderName]="chat.verifiedProjectFolder()?.name ?? null"
        [stagedAt]="chat.cloudStagedAt()"
        [threadId]="chat.threadId()"
        (review)="chat.cloudDiffPanelOpen.set(true)"
        (recheck)="chat.refreshCloudDiffCount()"
      />

      <!-- View menu: device-local display prefs (chatPrefs/localStorage).
           Session config (model, temperature, mode, tools, …) lives in the
           settings pane opened via settingsRequested — the old live rows
           were retired with it (live_session_settings.md, Slice A). -->
      @if (showViewMenu()) {
        <div class="settings-panel">
          <div class="settings-row">
            <label class="settings-label">{{ 'chat.settings.reasoning' | transloco }}</label>
            <app-select size="sm" [fullWidth]="false"
                        [value]="chatPrefs.reasoningExpanded() ? 'expanded' : 'collapsed'"
                        (changed)="onReasoningDefaultChange($event)">
              <option value="expanded">{{ 'chat.settings.reasoningExpanded' | transloco }}</option>
              <option value="collapsed">{{ 'chat.settings.reasoningCollapsed' | transloco }}</option>
            </app-select>
          </div>
          <div class="settings-row">
            <label class="settings-label">{{ 'chat.settings.toolCalls' | transloco }}</label>
            <app-select size="sm" [fullWidth]="false"
                        [value]="chatPrefs.toolCallsExpanded() ? 'expanded' : 'collapsed'"
                        (changed)="onToolCallsDefaultChange($event)">
              <option value="expanded">{{ 'chat.settings.toolCallsExpanded' | transloco }}</option>
              <option value="collapsed">{{ 'chat.settings.toolCallsCollapsed' | transloco }}</option>
            </app-select>
          </div>
          <div class="settings-row">
            <label class="settings-label">{{ 'chat.settings.readingWidth' | transloco }}</label>
            <app-select size="sm" [fullWidth]="false"
                        [value]="chatPrefs.readingWidth()"
                        (changed)="onReadingWidthChange($event)">
              <option value="comfortable">{{ 'chat.settings.widthComfortable' | transloco }}</option>
              <option value="wide">{{ 'chat.settings.widthWide' | transloco }}</option>
              <option value="full">{{ 'chat.settings.widthFull' | transloco }}</option>
            </app-select>
          </div>
          <div class="settings-row">
            <label class="settings-label">{{ 'chat.settings.textSize' | transloco }}</label>
            <app-select size="sm" [fullWidth]="false"
                        [value]="chatPrefs.textSize()"
                        (changed)="onTextSizeChange($event)">
              <option value="small">{{ 'chat.settings.textSmall' | transloco }}</option>
              <option value="medium">{{ 'chat.settings.textMedium' | transloco }}</option>
              <option value="large">{{ 'chat.settings.textLarge' | transloco }}</option>
            </app-select>
          </div>
          @if (chat.isOfficerThread()) {
            <div class="settings-row">
              <label class="settings-label">{{ 'chat.settings.officerLens' | transloco }}</label>
              <app-select size="sm" [fullWidth]="false"
                          [value]="chatPrefs.officerLensFolded() ? 'folded' : 'all'"
                          (changed)="onOfficerLensChange($event)">
                <option value="folded">{{ 'chat.settings.officerLensFolded' | transloco }}</option>
                <option value="all">{{ 'chat.settings.officerLensAll' | transloco }}</option>
              </app-select>
            </div>
          }
        </div>
      }

      <!-- Citations panel (Half-B v2): session citations + view-original/drift -->
      @if (showCitations()) {
        <div class="settings-panel citations-panel-wrap">
          <app-citations-panel (close)="showCitations.set(false)" />
        </div>
      }

      <!-- Connect over SSH (workspace_ssh_access.md §5.1). @defer'd on the
           toggle: the session view sits in the INITIAL bundle
           (app.routes.ts eagerly imports ChatPageComponent, which eagerly
           imports this component), and a plain import of the panel plus its
           stylesheet would land in that same chunk — which after Task 4
           measures 2.73MB against a 2.75MB hard-fail budget, ~20kB of
           headroom (ruling P-11). The panel only ever exists after a click,
           so deferring on that click is its natural shape, not a workaround. -->
      @defer (when showSshPanel()) {
        @if (showSshPanel()) {
          <div class="settings-panel">
            <app-ssh-connect-panel
              [handle]="chat.sshHandle()"
              [apiHost]="sshApiHost"
              [sshHost]="sshGatewayHostname()"
              [hostKeyFingerprint]="sshHostKeyFingerprint()"
            />
          </div>
        }
      }

      <!-- Cloud-diff review. Was an inline 70vh block wedged into the chat
           column with six hard-coded inline style= attributes, no dialog
           semantics and no focus management; it is now a proper modal
           (PC-23). @defer is load-bearing, not decoration: the review surface
           pulls Monaco's loader and is never needed on first paint, and the
           initial bundle is at 2.70MB against a 2.75MB hard-error budget. -->
      @defer (when chat.cloudDiffPanelOpen() || !!jobDiffId()) {
        @if (chat.cloudDiffPanelOpen()) {
          <app-cloud-review-dialog
            [open]="true"
            [threadId]="chat.threadId()"
            [projectFolder]="chat.verifiedProjectFolder()"
            (resolved)="chat.onCloudDiffResolved()"
            (closed)="chat.closeCloudReview()"
          />
        }

        <!-- Job-diff twin for a job card's "Open diff". Same component, bound
             to a jobId. Kept as a separate signal so opening a job's diff can
             never be confused with, or clobber, the session's own staged
             cloud changes. -->
        @if (jobDiffId(); as jobId) {
          <app-cloud-review-dialog [open]="true" [jobId]="jobId" (closed)="jobDiffId.set(null)" />
        }
      }

      <!-- Task bar -->
      @if (chat.tasks().length) {
        <div class="task-bar">
          <div class="task-header"
               [class.task-header-clickable]="chat.tasks().length > 1"
               (click)="chat.tasks().length > 1 ? toggleTasksCollapsed() : null">
            <app-icon size="sm" class="task-header-icon">checklist</app-icon>
            {{ 'chat.tasks.header' | transloco:{ done: completedTaskCount(), total: chat.tasks().length } }}
            @if (chat.tasks().length > 1) {
              <app-icon size="sm" class="task-chevron" [class.task-chevron-open]="!tasksCollapsed()">expand_more</app-icon>
            }
          </div>
          <div class="task-list">
            @if (chat.tasks().length <= 1 || !tasksCollapsed()) {
              @for (task of chat.tasks(); track task.id) {
                <div class="task-item" [class.task-completed]="task.status === 'completed'">
                  <app-icon size="sm" class="task-check">{{ task.status === 'completed' ? 'check_circle' : 'radio_button_unchecked' }}</app-icon>
                  <span class="task-desc">{{ task.description }}</span>
                </div>
              }
            } @else {
              @if (nextPendingTask(); as task) {
                <div class="task-item">
                  <app-icon size="sm" class="task-check">radio_button_unchecked</app-icon>
                  <span class="task-desc">{{ task.description }}</span>
                </div>
              } @else {
                <div class="task-item task-completed">
                  <app-icon size="sm" class="task-check" style="color: var(--success)">check_circle</app-icon>
                  <span class="task-desc">{{ 'chat.tasks.allCompleted' | transloco }}</span>
                </div>
              }
            }
          </div>
        </div>
      }

      <!--
        Shared template for the per-tool-call expandable card list. Used by
        three sites: the streaming "completed tools" block, the finalized
        message-with-content branch, and the tool-only-message branch. Keep
        the template the single source of truth — adding/changing args
        formatting, status icons, decision badges, etc. should only happen
        here.
      -->
      <ng-template #toolDetails let-tools>
        <div class="tool-detail-list">
          @for (tc of tools; track tc.id) {
            <app-tool-card [view]="toolView(tc)" (actionRequested)="canvasRequested.emit()"
                           (jobDiffRequested)="openJobDiff($event)" />
          }
        </div>
      </ng-template>

      <!-- Per-event card templates (referenced by the turn loop below). -->
      <ng-template #thoughtCard let-event>
        <details class="thinking-block event-thought" [attr.open]="chatPrefs.reasoningExpanded() ? '' : null">
          <summary class="thinking-header">
            <app-icon size="sm" class="thinking-icon">psychology</app-icon>
            <span class="thinking-label">
              {{ (event.status === 'streaming' ? 'chat.thinking.now' : 'chat.thinking.past') | transloco }}
            </span>
          </summary>
          <div class="thinking-content" [class.streaming-block]="event.status === 'streaming'">
            <markdown appCitationRef appKatex appWorkspaceFileLink [data]="event.content"
                      [katexDefer]="event.status === 'streaming'"
                      (workspaceFileOpen)="workspaceFileRequested.emit($event)"></markdown>
          </div>
        </details>
      </ng-template>

      <!-- Startup banner: slim, non-scrolling status strip shown once a turn
           exists while the session is still starting. Reuses the .startup-step
           row so it matches the centered card; replaces the old floating
           .startup-wrapper.resume overlay so the card never sits over the
           message list. Auto-hides when sessionReady flips. -->
      @if (startupBannerVisible()) {
        <div class="startup-banner">
          @if (currentStartupStep(); as step) {
            <div class="startup-step state-active">
              <span class="step-spinner" aria-hidden="true"></span>
              <span class="step-label">{{ step.labelKey | transloco }}</span>
              <time class="step-time">{{ formatElapsed(step.elapsedMs) }}</time>
            </div>
          }
        </div>
      }

      <!-- Messages -->
      <div class="messages"
           #messagesContainer
           (scroll)="onMessagesScroll()"
           [attr.data-testid]="chat.isStartingSession() ? 'chat-startup' : null">
        <!-- Centered reading column: caps prose line length while the scrollbar
             stays at the pane edge. .jump-latest is kept OUTSIDE this wrapper so
             it floats over the scroll container (sticky + align-self:center). -->
        <div class="messages-inner" #messagesInner>
        @for (view of turnViews(); track view.id; let isLast = $last) {
          @if (view.kind === 'wake_cycle') {
            <details class="message message-system wake-cycle" data-testid="wake-cycle">
              <summary class="system-message wake-cycle-summary">
                <app-icon size="sm" class="system-icon">bedtime</app-icon>
                {{ 'chat.settings.wakeLine' | transloco:{
                    time: (view.sitrep.timestamp | date:'HH:mm'),
                    minutes: view.minutes ?? '?',
                    reason: view.reason || ('chat.settings.wakeNoReason' | transloco)
                  } }}
              </summary>
              <div class="wake-cycle-body">
                <pre class="wake-cycle-sitrep">{{ view.sitrep.content }}</pre>
                @if (trailingText(view.wake); as said) {
                  <div class="wake-cycle-said">{{ said }}</div>
                }
                <div class="wake-cycle-sleep">
                  <app-icon size="sm">bedtime</app-icon>
                  {{ 'chat.settings.wakeSleepLine' | transloco:{
                      minutes: view.minutes ?? '?',
                      reason: view.reason || ('chat.settings.wakeNoReason' | transloco)
                    } }}
                </div>
              </div>
            </details>
          } @else {
          @let turn = view.turn;
          @switch (turn.kind) {
            @case ('system') {
              <div class="message message-system">
                <div class="system-message">
                  <app-icon size="sm" class="system-icon">info</app-icon>
                  {{ turn.content }}
                </div>
              </div>
            }
            @case ('compaction') {
              <!-- Compaction boundary: divider banner (reuses .session-divider),
                   expandable to the summary so the user sees the agent's state. -->
              <div class="session-divider">
                <span class="divider-line"></span>
                <span class="divider-text">{{ 'chat.compaction.banner' | transloco }}</span>
                <span class="divider-line"></span>
              </div>
              @if (turn.summary) {
                <details class="compaction-summary">
                  <summary>{{ 'chat.compaction.viewSummary' | transloco }}</summary>
                  <!-- The agent's summary is markdown (headings, lists, code). Render it
                       as such, reusing the chat's markdown cascade via the message-body
                       class (its base styles are benign; the bubble styling is gated on
                       .message-user/.message-assistant, which this isn't). -->
                  <div class="compaction-summary-body message-body">
                    <markdown appKatex appWorkspaceFileLink [data]="turn.summary"
                              (workspaceFileOpen)="workspaceFileRequested.emit($event)"></markdown>
                  </div>
                </details>
              }
            }
            @case ('user') {
              @let queued = chat.outboxIds().has(turn.id);
              @let stalled = queued && chat.outboxStalled();
              <div class="message message-user"
                   data-testid="chat-message-user"
                   [class.historical]="turn.historical"
                   [class.queued]="queued"
                   [class.stalled]="stalled"
                   [attr.aria-busy]="queued ? 'true' : null">
                <div class="avatar">
                  @if (stalled) {
                    <!-- Not "waiting to send" — the send actually failed. -->
                    <app-icon size="sm" class="avatar-icon"
                              [title]="'chat.queued.notSent' | transloco">error_outline</app-icon>
                  } @else if (queued) {
                    <app-icon size="sm" class="avatar-icon"
                              [title]="'chat.queued.waiting' | transloco">schedule</app-icon>
                  } @else {
                    <app-icon size="sm" class="avatar-icon">person</app-icon>
                  }
                </div>
                <div class="message-body">
                  @if (turn.content) {
                    @if (isUserMessageLong(turn.content)) {
                      <details class="user-text-collapsible">
                        <summary class="user-text-summary">
                          <span class="user-text-preview">{{ userMessagePreview(turn.content) }}</span>
                          <span class="user-text-hint user-text-hint-closed">[…]</span>
                          <span class="user-text-hint user-text-hint-open">▴</span>
                        </summary>
                        <div class="user-text">{{ turn.content }}</div>
                      </details>
                    } @else {
                      <div class="user-text">{{ turn.content }}</div>
                    }
                  }
                  @if (turn.attachments?.length) {
                    <div class="user-attachments">
                      @for (att of turn.attachments; track att.id) {
                        <span class="user-attachment-chip" [title]="att.path ?? att.name">
                          <app-icon size="sm">{{
                            att.mimeType.startsWith('image/') ? 'image' :
                            att.mimeType.startsWith('video/') ? 'videocam' :
                            att.mimeType.startsWith('audio/') ? 'audiotrack' :
                            'description'
                          }}</app-icon>
                          <span class="user-attachment-name">{{ att.name }}</span>
                        </span>
                      }
                    </div>
                  }
                  @if (queued && !stalled) {
                    @let stage = uploadStage(turn.id);
                    @if (stage) {
                      <div class="upload-stage">
                        <div class="upload-stage-line">
                          <app-icon size="sm">upload</app-icon>
                          <!-- Visible label carries the percentage; it changes
                               ~4×/s, so it is hidden from the a11y tree and the
                               polite region beside it announces the coarse
                               phase only (see uploadStageAnnounceKey). -->
                          <span aria-hidden="true">{{ stage.key | transloco: stage.params }}</span>
                          <span class="upload-stage-announce"
                                aria-live="polite">{{ stage.announceKey | transloco: stage.params }}</span>
                        </div>
                        @if (stage.bar) {
                          <!-- ONE indicator spanning the upload AND the POST:
                               it never resets between the two and never reaches
                               100%, because the accepted send removes this
                               bubble instead of filling the bar. No
                               aria-valuenow => indeterminate; aria-valuetext
                               carries the phase, since a bare "90%" while
                               "Sending…" would mislead. progressbar is not a
                               live region and announces nothing on its own. -->
                          <div class="upload-bar"
                               [class.indeterminate]="stage.percent === null"
                               role="progressbar"
                               [attr.aria-label]="'chat.upload.progressLabel' | transloco"
                               aria-valuemin="0"
                               aria-valuemax="100"
                               [attr.aria-valuenow]="stage.percent"
                               [attr.aria-valuetext]="stage.key | transloco: stage.params">
                            <div class="upload-bar-fill" [style.width.%]="stage.percent"></div>
                          </div>
                        }
                      </div>
                    }
                  }
                  <!-- Stalled queue: the flush has no timed auto-retry, so
                       without these the bubble spins on "sending" forever. -->
                  @if (stalled) {
                    @let requiresReview = chat.outboxItem(turn.id)?.requiresReview;
                    <div class="queued-actions">
                      <span class="queued-note">{{ (requiresReview ? 'chat.rewind.staleOutbox' : 'chat.queued.notSent') | transloco }}</span>
                      @if (requiresReview) {
                        <button type="button" class="queued-action"
                                (click)="reviewQueuedSend(turn.id)">{{ 'chat.rewind.reviewDraft' | transloco }}</button>
                      } @else {
                        <button type="button" class="queued-action"
                                (click)="chat.retryQueuedSends()">{{ 'chat.queued.retry' | transloco }}</button>
                      }
                      <button type="button" class="queued-action"
                              (click)="chat.discardQueuedSend(turn.id)">{{ 'chat.queued.discard' | transloco }}</button>
                    </div>
                  }
                </div>
                @if (turn.historical && !chat.outboxIds().has(turn.id) && chat.rewindModeAvailable('conversation')) {
                  <button type="button"
                          class="rewind-btn"
                          [attr.aria-label]="'chat.rewind.button' | transloco"
                          [title]="'chat.rewind.button' | transloco"
                          (click)="openRewindSheet(turn)">
                    <app-icon size="sm">history</app-icon>
                  </button>
                }
              </div>
            }
            @case ('assistant') {
              @let isCollapsed = isTurnCollapsed(turn);
              @let counts = turnEventCounts(turn);
              @let last = lastTextEvent(turn);
              @let streaming = turn.status === 'streaming';
              <div class="message message-assistant turn-bubble"
                   data-testid="chat-message-assistant"
                   [class.historical]="turn.historical"
                   [class.streaming]="streaming"
                   [class.collapsed]="isCollapsed"
                   [class.dimmed]="isShowingReconnectBanner() && isLast">
                <div class="avatar">
                  <app-icon size="sm" class="avatar-icon">smart_toy</app-icon>
                </div>
                <div class="message-body turn-body">
                  <!-- Whole-turn chevron: folds the lead-up (reasoning + tool
                       calls) behind the per-type badge summary, leaving the
                       final answer visible. Hidden when the turn has 0–1 events
                       (nothing to collapse). -->
                  @if (turn.events.length > 1) {
                    <button type="button"
                            class="turn-chevron"
                            [attr.aria-expanded]="!isCollapsed"
                            (click)="toggleTurnCollapse(turn)">
                      <app-icon size="sm" class="turn-chevron-icon">{{ isCollapsed ? 'chevron_right' : 'expand_more' }}</app-icon>
                      <span class="turn-chevron-badge">
                        @if (counts.thoughts > 0) {
                          <span class="badge-thought" [title]="('chat.turn.thoughtCount' | transloco:{count: counts.thoughts})">◐ {{ counts.thoughts }}</span>
                        }
                        @if (counts.texts > 0 && !isCollapsed) {
                          <span class="badge-text" [title]="('chat.turn.textCount' | transloco:{count: counts.texts})">✎ {{ counts.texts }}</span>
                        }
                        @if (counts.tools > 0) {
                          <span class="badge-tool" [title]="('chat.turn.toolCount' | transloco:{count: counts.tools})">▶ {{ counts.tools }}</span>
                        }
                      </span>
                    </button>
                  }

                  @if (isCollapsed) {
                    <!-- Collapsed: fold the lead-up (opening text, reasoning,
                         tool calls) but keep the final answer — the trailing
                         prose (stray finished thoughts / compaction markers
                         after it are tolerated) — fully rendered as markdown
                         (#8 refinement). The chevron + count badge signal the
                         hidden work. When the turn has no closing prose (ends
                         on a tool call, or is still thinking), fall back to a
                         one-line headline (plain text so the truncate mixin
                         works; markdown emits block elements that defeat
                         nowrap). -->
                    <!-- Officer→user messages stay visible even collapsed:
                         collapsing folds the lead-up, and a message addressed
                         to the user is never lead-up. Chronological, before
                         the closing prose. -->
                    @for (nc of collapsedNotifyCalls(turn); track nc.id) {
                      <div class="event-tool">
                        <ng-container [ngTemplateOutlet]="toolDetails" [ngTemplateOutletContext]="{ $implicit: [nc] }"></ng-container>
                      </div>
                    }
                    @let answer = finalAnswer(turn);
                    @if (answer) {
                      <!-- A user can collapse a still-streaming turn (the manual
                           override wins over the streaming check), so this path
                           renders growing text too — gate its DOM post-processing
                           + KaTeX off the turn's streaming status. -->
                      <div class="event-text turn-final-answer" [class.streaming-block]="streaming">
                        <markdown appCitationRef appKatex appWorkspaceFileLink [data]="answer" [katexDefer]="streaming"
                                  (workspaceFileOpen)="workspaceFileRequested.emit($event)"></markdown>
                      </div>
                    } @else {
                      <span class="turn-headline">{{ collapsedHeadline(turn) }}</span>
                    }
                  } @else {
                    <!-- Expanded: the live edge renders as cards (anything in
                         flight, plus the turn's latest tool call) and everything
                         already finished folds into one chip. Text never folds. -->
                    @for (group of groupedEvents(turn); track group.id) {
                      @if (group.kind === 'folded') {
                        @if (foldRun(group.events)) {
                          <!-- Folded: a category count line, not a tool list. Does NOT
                               auto-open on failure — with a turn-wide chip that would dump
                               every card because one call errored; the ⚠ badge carries it
                               instead, and a just-failed call is pinned anyway. Suppressed
                               entirely when Tool calls → Expanded (everything inline). -->
                          <details class="tool-group">
                            <summary class="tool-group-head">
                              <app-icon size="sm" class="tool-group-chevron">chevron_right</app-icon>
                              <span class="tool-group-label">{{ foldedSummaryText(group.events) }}</span>
                              @if (foldedFailedCount(group.events); as failed) {
                                <span class="tool-group-failed">{{ 'chat.turn.foldFailed' | transloco:{count: failed} }}</span>
                              }
                            </summary>
                            <div class="tool-group-body">
                              @for (event of group.events; track event.id) {
                                @if (event.kind === 'tool_call') {
                                  <ng-container [ngTemplateOutlet]="toolDetails" [ngTemplateOutletContext]="{ $implicit: [event] }"></ng-container>
                                } @else if (chat.narrationMode() !== 'silent') {
                                  <ng-container [ngTemplateOutlet]="thoughtCard" [ngTemplateOutletContext]="{ $implicit: event }"></ng-container>
                                }
                              }
                            </div>
                          </details>
                        } @else {
                          <!-- Tool calls → Expanded, or a run too short to be worth a chip. -->
                          @for (event of group.events; track event.id) {
                            @if (event.kind === 'tool_call') {
                              <div class="event-tool">
                                <ng-container [ngTemplateOutlet]="toolDetails" [ngTemplateOutletContext]="{ $implicit: [event] }"></ng-container>
                              </div>
                            } @else if (chat.narrationMode() !== 'silent') {
                              <ng-container [ngTemplateOutlet]="thoughtCard" [ngTemplateOutletContext]="{ $implicit: event }"></ng-container>
                            }
                          }
                        }
                      } @else if (group.kind === 'job_batch') {
                        <!-- A fan-out: one card, a row per job. Job calls never
                             fold (they carry live status and review actions), so
                             without this a three-job dispatch rendered a "2×
                             tool calls" chip plus one card. -->
                        <div class="event-tool">
                          <app-job-batch-card [views]="jobBatchViews(group)"
                                              (diffRequested)="openJobDiff($event)" />
                        </div>
                      } @else {
                        @switch (group.event.kind) {
                          @case ('tool_call') {
                            <!-- Pinned: in flight, or the turn's latest call. -->
                            <div class="event-tool">
                              <ng-container [ngTemplateOutlet]="toolDetails" [ngTemplateOutletContext]="{ $implicit: [group.event] }"></ng-container>
                            </div>
                          }
                          @case ('thought') {
                            @if (chat.narrationMode() !== 'silent') {
                              <ng-container [ngTemplateOutlet]="thoughtCard" [ngTemplateOutletContext]="{ $implicit: group.event }"></ng-container>
                            }
                          }
                          @case ('text') {
                            <div class="event-text" [class.streaming-block]="group.event.status === 'streaming'">
                              <markdown appCitationRef appKatex appWorkspaceFileLink [data]="group.event.content"
                                        [katexDefer]="group.event.status === 'streaming'"
                                        (workspaceFileOpen)="workspaceFileRequested.emit($event)"></markdown>
                            </div>
                          }
                          @case ('compaction') {
                            <!-- Mid-turn compaction marker at its true position
                                 in the event stream (same divider + expandable
                                 summary as the between-turns banner). -->
                            <div class="session-divider event-compaction">
                              <span class="divider-line"></span>
                              <span class="divider-text">{{ 'chat.compaction.banner' | transloco }}</span>
                              <span class="divider-line"></span>
                            </div>
                            @if (group.event.summary) {
                              <details class="compaction-summary">
                                <summary>{{ 'chat.compaction.viewSummary' | transloco }}</summary>
                                <div class="compaction-summary-body message-body">
                                  <markdown appKatex appWorkspaceFileLink [data]="group.event.summary"
                                            (workspaceFileOpen)="workspaceFileRequested.emit($event)"></markdown>
                                </div>
                              </details>
                            }
                          }
                        }
                      }
                    }

                    <!-- Streaming pulse while the turn is in flight with nothing yet. -->
                    @if (streaming && turn.events.length === 0 && chat.pendingPermissions().length === 0) {
                      <div class="thinking">
                        <span class="thinking-dot"></span>
                        <span class="thinking-dot"></span>
                        <span class="thinking-dot"></span>
                      </div>
                    }
                  }

                  <!-- Read aloud (Phase 1): the button, staged status box, and
                       players are all owned by <app-read-aloud>. -->
                  @if (last && !streaming) {
                    <app-read-aloud [content]="last.content" [threadId]="chat.threadId()" />
                  }
                </div>
              </div>
            }
          }
          }
          <!-- Divider between historical-loaded turns and the live session. -->
          @if (showSessionDividerAfterView(view, $index)) {
            <div class="session-divider">
              <span class="divider-line"></span>
              <span class="divider-text">{{ 'chat.system.sessionResumed' | transloco }}</span>
              <span class="divider-line"></span>
            </div>
          }
        } @empty {
          @if (!chat.isStreaming()) {
            <div class="empty-state">
              @if (chat.sessionReady()) {
                <app-chat-empty-state
                  variant="ready"
                  [suggestions]="displayedSuggestions()"
                  (suggestionPicked)="pickSuggestion($event)"
                />
              } @else if (chat.isDraftSession()) {
                <app-chat-empty-state
                  variant="draft"
                  [suggestions]="displayedSuggestions()"
                  [connectorsLoading]="chat.draftDefaultsLoading()"
                  [connectorsError]="chat.draftDefaultsError()"
                  [connectorsEnabled]="chat.draftConnectorsEnabled()"
                  [datasourceCount]="chat.draftDatasourceIds()?.length ?? 0"
                  (suggestionPicked)="pickSuggestion($event)"
                  (connectorsToggled)="chat.setDraftConnectorsEnabled($event)"
                  (retryRequested)="chat.retryDraftDefaults()"
                />
              } @else if (chat.isStartingSession()) {
                <div class="startup-wrapper">
                  <ng-container *ngTemplateOutlet="startupCardTpl"></ng-container>
                </div>
              }
            </div>
          }
        }

        @if (chat.isDraftSession() && chat.outbox().length > 0) {
          <app-chat-empty-state
            variant="recovery"
            [suggestions]="[]"
            [connectorsLoading]="chat.draftDefaultsLoading()"
            [connectorsError]="chat.draftDefaultsError()"
            [connectorsEnabled]="chat.draftConnectorsEnabled()"
            [datasourceCount]="chat.draftDatasourceIds()?.length ?? 0"
            [workspaceBackend]="chat.draftWorkspaceBackend()"
            (connectorsToggled)="chat.setDraftConnectorsEnabled($event)"
            (workspaceChanged)="chat.setDraftWorkspaceBackend($event)"
            (retryRequested)="chat.retryDraftDefaults()"
            (sendRetried)="chat.retryDraftSession()"
          />
        }

        <!-- Parked unit: the input was accepted but nothing can claim it until
             it is retried (attach failures, a maintenance interruption, a
             settlement failure). Rendered from the durable queue block, so a
             reload shows it too. Never the waiting/busy copy: this is a
             failure with a way out, not a queue. -->
        @if (chat.isParked()) {
          <div class="message message-assistant turn-bubble parked-turn" role="alert" data-testid="chat-parked">
            <div class="avatar">
              <app-icon size="sm" class="avatar-icon parked-icon">error_outline</app-icon>
            </div>
            <div class="message-body turn-body parked-body">
              <div class="parked-line">{{ 'chat.parked.title' | transloco }}</div>
              <div class="parked-reason">{{ parkedReasonKey() | transloco }}</div>
              @if (chat.queueState()?.retryable) {
                <div class="parked-actions">
                  <app-button variant="ghost" size="sm" data-testid="chat-parked-retry"
                              [disabled]="retryingParked()"
                              (clicked)="retryParked()">
                    {{ (retryingParked() ? 'chat.parked.retrying' : 'chat.parked.retry') | transloco }}
                  </app-button>
                </div>
              }
            </div>
          </div>
        }

        <!-- Accepted-but-not-yet-started turn: the input is durably queued
             but no agent has claimed it yet (pool busy, or the previous
             turn's cloud push still flushing). Shown standalone because no
             turn object exists until turn.started, and deliberately NOT the
             thinking dots: queued is not working. Escalates after 10 s /
             60 s; never shows a queue position. -->
        @if (chat.isAwaitingTurn()) {
          <div class="message message-assistant turn-bubble queued-turn" role="status" aria-live="polite">
            <div class="avatar">
              <app-icon size="sm" class="avatar-icon queued-icon">hourglass_top</app-icon>
            </div>
            <div class="message-body turn-body queued-body">
              <div class="queued-line">{{ 'chat.queue.waiting' | transloco }}</div>
              @if (queuedTier() !== 'fresh') {
                <div class="queued-busy">{{ 'chat.queue.busy' | transloco }}</div>
              }
              @if (queuedTier() === 'long') {
                <div class="queued-elapsed">{{ 'chat.queue.elapsed' | transloco: {time: queuedWaitLabel()} }}</div>
              }
            </div>
          </div>
        }

        <ng-template #startupCardTpl>
          <div class="startup-card">
            <div class="startup-card-head">
              <span class="startup-card-spinner"></span>
              <span class="startup-card-title">
                {{ (chat.turns().length > 0 ? 'chat.startup.titleResume' : 'chat.startup.title') | transloco }}
              </span>
            </div>
            <div class="startup-steps">
              @for (step of startupSteps(); track step.key) {
                <div class="startup-step" [class]="'state-' + step.state">
                  @if (step.state === 'active') {
                    <span class="step-spinner" aria-hidden="true"></span>
                  } @else {
                    <app-icon size="lg" class="step-icon">{{ stepIcon(step.state) }}</app-icon>
                  }
                  <span class="step-label">{{ step.labelKey | transloco }}</span>
                  <time class="step-time">{{ formatElapsed(step.elapsedMs) }}</time>
                </div>
              }
            </div>
          </div>
        </ng-template>

        <!-- Inline approval card (mile marker) — anchored to the live turn,
             not gated on streaming state so it stays visible across edge cases. -->
        @if (chat.pendingPermissions().length > 0) {
          <div class="mile mile-permission">
            <div class="mile-label">{{ 'chat.permission.title' | transloco }}</div>
            <div class="mile-title">
              {{ permissionTitleKey(chat.pendingPermissions().length) | transloco: {count: chat.pendingPermissions().length} }}
            </div>
            <ul class="permission-list">
              @for (perm of chat.pendingPermissions(); track perm.id) {
                <li class="permission-row">
                  <app-icon size="sm">{{ toolIcon(perm.tool) }}</app-icon>
                  <code class="permission-tool">{{ perm.tool }}</code>
                  <code class="permission-args">{{ permissionArgs(perm) }}</code>
                </li>
              }
            </ul>
            <div class="mile-actions">
              <app-button variant="success" size="sm" (clicked)="chat.approveAll()">{{ permissionApproveKey(chat.pendingPermissions().length) | transloco }}</app-button>
              <app-button variant="info" size="sm" (clicked)="approveAndAutoAccept()">{{ 'chat.permission.autoAccept' | transloco }}</app-button>
              <app-button variant="danger" size="sm" (clicked)="chat.stop()">{{ 'chat.permission.stop' | transloco }}</app-button>
            </div>
          </div>
        }

        <!-- Running-command card — shown on (re)attach when the agent is
             blocked in a tool call that isn't already rendered in a visible
             turn (cold reload mid-turn: the in-flight turn isn't in REST
             history yet). Reuses the .mile marker styles (no new SCSS). -->
        @if (runningCommandCard(); as rc) {
          <div class="mile">
            <div class="mile-label">{{ 'chat.stream.running' | transloco:{ tool: rc.tool } }}</div>
            <div class="mile-detail">
              <app-icon size="sm" class="mile-detail-icon">progress_activity</app-icon>
              @if (formatToolArgs(rc.args); as a) {
                <code class="mile-args">{{ a }}</code>
              }
            </div>
            <div class="mile-title">{{ 'chat.stream.waitingForCommand' | transloco }}</div>
          </div>
        }

        <!-- Agent-initiated workspace upgrade: the offer, and the provisioning
             it turns into. Reuses the .mile marker styles (no new SCSS). Not
             gated on streaming — request_workspace_upgrade doesn't stop the
             turn, so this appears while the agent is still talking. -->
        @if (workspaceOfferCard(); as woc) {
          <div class="mile">
            @switch (woc.state) {
              @case ('offer') {
                <div class="mile-label">{{ 'chat.workspaceOffer.title' | transloco }}</div>
                <div class="mile-title">{{ 'chat.workspaceOffer.detail' | transloco:{ tier: woc.tier } }}</div>
                <div class="mile-detail">
                  <app-icon size="sm" class="mile-detail-icon">terminal</app-icon>
                  <span class="mile-args">{{ woc.reason }}</span>
                </div>
                <div class="mile-actions">
                  <app-button variant="success" size="sm" (clicked)="upgradeAndContinue(woc.tier)">
                    {{ 'chat.workspaceOffer.upgradeAndContinue' | transloco }}
                  </app-button>
                  <app-button variant="info" size="sm" (clicked)="upgradeOnly(woc.tier)">
                    {{ 'chat.workspaceOffer.upgrade' | transloco }}
                  </app-button>
                  <app-button variant="danger" size="sm" (clicked)="denyOffer()">
                    {{ 'chat.workspaceOffer.deny' | transloco }}
                  </app-button>
                </div>
              }
              @case ('provisioning') {
                <div class="mile-label">{{ 'chat.workspaceOffer.title' | transloco }}</div>
                <div class="mile-detail">
                  <span class="action-spinner-sm" aria-hidden="true"></span>
                  <span class="mile-args">
                    {{ 'chat.workspaceOffer.provisioning' | transloco:{ tier: woc.tier } }}
                    @if (woc.elapsed) { ({{ woc.elapsed }}s) }
                  </span>
                </div>
                @if (woc.willContinue) {
                  <div class="mile-title">{{ 'chat.workspaceOffer.willContinue' | transloco }}</div>
                }
              }
            }
          </div>
        }

        <!-- Live context-compaction progress (compaction.started/progress
             frames; cleared by context.compacted / compaction.failed).
             Segmented bar for ≤20 passes, continuous bar + counter above —
             the UI must render fine for ANY pass count. -->
        @if (chat.compaction(); as comp) {
          <div class="compaction-progress" role="status">
            <div class="compaction-header">
              <span class="action-spinner-sm" aria-hidden="true"></span>
              <span class="compaction-title">{{ 'chat.compactionLive.title' | transloco }}</span>
              <span class="compaction-meta">
                {{ comp.trigger }}
                @if (comp.ctxUsedPct != null) {
                  · {{ 'chat.compactionLive.ctx' | transloco:{ pct: comp.ctxUsedPct } }}
                }
              </span>
              @if (comp.ctxUsedTokens != null && comp.ctxLimitTokens != null) {
                <span class="compaction-tokens">{{ formatTokens(comp.ctxUsedTokens) }} / {{ formatTokens(comp.ctxLimitTokens) }} tok</span>
              }
            </div>
            <div class="compaction-bar">
              @if (compactionSegments().length) {
                @for (seg of compactionSegments(); track seg) {
                  <span class="compaction-segment"
                        [class.done]="seg < comp.currentPass"
                        [class.active]="seg === comp.currentPass"></span>
                }
              } @else {
                <span class="compaction-segment continuous">
                  <span class="compaction-fill"
                        [style.width.%]="comp.nPasses > 0 ? (100 * (comp.currentPass > 0 ? comp.currentPass - 1 : 0) / comp.nPasses) : 0"></span>
                </span>
              }
            </div>
            <div class="compaction-detail">
              <span class="compaction-pass">
                @if (comp.currentPass > 0) {
                  {{ 'chat.compactionLive.pass' | transloco:{ current: comp.currentPass, total: comp.nPasses, first: comp.firstMsg ?? '?', last: comp.lastMsg ?? '?' } }}
                  @if (comp.attempt > 1) {
                    <span class="compaction-retry">{{ 'chat.compactionLive.retry' | transloco:{ attempt: comp.attempt } }}</span>
                  }
                } @else {
                  {{ 'chat.compactionLive.planning' | transloco }}
                }
              </span>
              @if (comp.inTokens != null) {
                <span class="compaction-reduction">
                  {{ formatTokens(comp.inTokens) }} →
                  @if (comp.outTokens != null) {
                    {{ formatTokens(comp.outTokens) }}
                  } @else {
                    …
                  }
                </span>
              }
            </div>
          </div>
        }

        @if (chat.threadStatus() === 'ending') {
          <div class="resume-card ending-card" role="status" aria-live="polite">
            <div class="resume-body">
              <div class="resume-eyebrow">{{ 'chat.ending.eyebrow' | transloco }}</div>
              <h3 class="resume-title">{{ 'chat.ending.title' | transloco }}</h3>
              <p class="resume-text">{{ 'chat.ending.body' | transloco }}</p>
            </div>
            <span class="ide-spinner" aria-hidden="true"></span>
          </div>
        }

        <!-- Ended-session end-marker + resume card. Sits at the tail of the
             transcript, directly above the (still-live) composer — the card is
             the resume-without-typing path; sending resumes too. -->
        @if (chat.threadStatus() === 'ended') {
          <div class="end-marker">
            <span class="end-line"></span>
            <div class="end-tag">
              <app-icon size="sm" class="end-icon">flag</app-icon>
              <span>{{ 'chat.ended.endedAt' | transloco:{ date: formatEndedAt(chat.endedAt()) } }}</span>
            </div>
            <span class="end-line"></span>
          </div>
          <div class="resume-card">
            <div class="resume-body">
              <div class="resume-eyebrow">{{ 'chat.ended.eyebrow' | transloco }}</div>
              <h3 class="resume-title">{{ 'chat.ended.title' | transloco }}</h3>
              <p class="resume-text">{{ 'chat.ended.body' | transloco }}</p>
            </div>
            <div class="resume-actions">
              <app-button variant="primary"
                          [loading]="chat.isResuming()"
                          (clicked)="resumeSession()">
                @if (chat.isResuming()) {
                  {{ 'chat.system.resuming' | transloco }}
                } @else {
                  <app-icon size="sm" class="resume-icon">play_arrow</app-icon>
                  {{ 'chat.ended.resume' | transloco }}
                }
              </app-button>
            </div>
          </div>
        }

        <!-- Reconnect banner: WS dropped on a still-active thread.
             Mutually exclusive with the F3 resume card (threadStatus !== 'ended')
             and the F2 startup card (sessionReady === true). -->
        @if (isShowingReconnectBanner()) {
          <div class="reconnect-banner">
            <app-icon size="sm" class="rb-icon">cloud_off</app-icon>
            <div class="rb-body">
              <strong>{{ 'chat.disconnected.title' | transloco }}</strong>
              @if (chat.reconnectGaveUp()) {
                <span>{{ 'chat.disconnected.gaveUp' | transloco }}</span>
              } @else if (chat.reconnectAttempt() > 0) {
                <span>{{ 'chat.disconnected.retrying' | transloco:{ attempt: chat.reconnectAttempt(), max: chat.reconnectMaxAttempts } }}</span>
              } @else {
                <span>{{ 'chat.disconnected.dropped' | transloco }}</span>
              }
            </div>
            <app-button variant="secondary" size="sm" (clicked)="chat.reconnectNow()">
              {{ (chat.reconnectGaveUp() ? 'chat.disconnected.reconnect' : 'chat.disconnected.retryNow') | transloco }}
            </app-button>
          </div>
        }

        </div><!-- /.messages-inner -->

        <!-- Jump-to-latest pill: appears when the user has scrolled up while
             new messages arrive. Sticky-positioned so it floats over the stream
             without needing a wrapper element. -->
        @if (showJumpToLatest()) {
          <button class="jump-latest" type="button" (click)="jumpToLatest()">
            <app-icon size="sm">arrow_downward</app-icon>
            <span>{{ 'chat.jumpToLatest' | transloco: { count: newMessageCount() } }}</span>
          </button>
        }
      </div>

      <!-- Error banner -->
      @if (chat.error(); as err) {
        @if (!isShowingReconnectBanner()) {
          <div class="error-banner" data-testid="chat-error" role="alert">
            <app-icon size="sm" class="error-icon">error</app-icon>
            {{ err }}
            <button class="error-dismiss" (click)="chat.error.set(null)">{{ 'chat.error.dismiss' | transloco }}</button>
          </div>
        }
      }

      <!-- Input. Rendered on an ended session too: the user can draft before
           bringing the agent back, and the send is what resumes (see
           canComposeDuringSession + persistent-chat.service.sendMessage).
           This block used to be removed entirely on 'ended', which stranded a
           half-typed message as unreadable, uneditable state. -->
      <div class="composer-wrap">
        <!-- Live token telemetry (usage.updated frames): latest context fill
             + cumulative output/reasoning for the running turn. -->
        @if (chat.currentUsage(); as u) {
          <div class="usage-panel" aria-hidden="true"
               [class.lvl-warn]="usageCtxLevel() === 'warn'"
               [class.lvl-danger]="usageCtxLevel() === 'danger'">
            <!-- secondary: per-turn token breakdown (compact chips) -->
            <span class="usage-tokens">
              @if (u.inputTokens != null) {
                <span class="usage-chip usage-chip--input"><span class="usage-k">{{ 'chat.usage.input' | transloco }}</span>{{ formatTokens(u.inputTokens) }}</span>
              }
              <span class="usage-chip"><span class="usage-k">{{ 'chat.usage.output' | transloco }}</span>{{ formatTokens(u.outputTokensTurn) }}</span>
              @if (u.reasoningTokensTurn > 0) {
                <span class="usage-chip usage-chip--reasoning" [title]="u.reasoningEstimated ? ('chat.usage.reasoningEstimatedHint' | transloco) : ''">
                  <span class="usage-k">{{ 'chat.usage.reasoning' | transloco }}</span>{{ u.reasoningEstimated ? '~' : '' }}{{ formatTokens(u.reasoningTokensTurn) }}
                </span>
              }
            </span>
            <!-- primary: context-window fill — the actionable metric, colour-ramped -->
            @if (usageCtxPct() != null) {
              <span class="usage-ctx" [title]="'chat.usage.ctxHint' | transloco">
                <span class="usage-ctx-label">{{ 'chat.usage.ctx' | transloco }}</span>
                <span class="usage-gauge"><span class="usage-gauge-fill" [style.width.%]="usageCtxPct()"></span></span>
                <span class="usage-ctx-pct">{{ usageCtxPct() }}%</span>
              </span>
            }
          </div>
        }
        <div
          class="composer"
          [class.focused]="inputFocused()"
          [class.disabled]="!canCompose()"
          [class.recording]="isRecording()"
        >
          <!-- Slash command autocomplete -->
          @if (showSlashMenu() && filteredCommands().length > 0) {
            <div class="slash-menu">
              @for (cmd of filteredCommands(); track cmd.command) {
                <div
                  class="slash-item"
                  [class.selected]="$index === slashSelectedIndex()"
                  (click)="selectSlashCommand(cmd)"
                  (mouseenter)="slashSelectedIndex.set($index)"
                >
                  <span class="slash-cmd">{{ cmd.command }}</span>
                  <span class="slash-desc">{{ cmd.descriptionKey | transloco }}</span>
                </div>
              }
            </div>
          }

          <!-- Attachment preview chips -->
          @if (chat.pendingAttachments().length > 0 && !isRecording()) {
            <div class="attachment-row">
              @for (preview of chat.pendingAttachments(); track preview.id) {
                <div class="attachment-chip" [class.is-image]="preview.type === 'image'">
                  @if (preview.type === 'image' && preview.preview) {
                    <button
                      type="button"
                      class="attachment-thumb"
                      (click)="openImagePreview(preview)"
                      [attr.aria-label]="preview.name"
                    >
                      <img [src]="preview.preview" [alt]="preview.name" />
                    </button>
                  } @else {
                    <span class="attachment-icon">
                      <app-icon size="sm">{{
                        preview.type === 'audio' ? 'audiotrack' :
                        preview.type === 'video' ? 'videocam' :
                        preview.type === 'document' ? 'description' : 'insert_drive_file'
                      }}</app-icon>
                    </span>
                  }
                  <span class="attachment-meta">
                    <span class="attachment-name">{{ preview.name }}</span>
                    <span class="attachment-size">{{ preview.sizeFormatted }}</span>
                  </span>
                  <button
                    type="button"
                    class="attachment-remove"
                    (click)="removeAttachment(preview.id)"
                    [attr.aria-label]="'chat.composer.remove' | transloco"
                    [title]="'chat.composer.remove' | transloco"
                  >
                    <app-icon size="sm">close</app-icon>
                  </button>
                </div>
              }
            </div>
          }

          <!-- Upload error banner -->
          @if (chat.attachmentError(); as err) {
            <div class="attachment-error">
              <app-icon size="sm">error_outline</app-icon>
              <span>{{ err }}</span>
            </div>
          }

          <!-- Transcribing indicator: brief, after a recording is confirmed -->
          @if (isTranscribing()) {
            <div class="transcribing-hint">
              <span class="action-spinner-sm" aria-hidden="true"></span>
              <span>{{ 'chat.composer.transcribing' | transloco }}</span>
            </div>
          }

          <!-- Recording mode: waveform + duration + controls -->
          @if (isRecording()) {
            <div class="recording-strip">
              <button
                type="button"
                class="recording-btn cancel"
                (click)="cancelRecording()"
                [attr.aria-label]="'chat.composer.recordingCancel' | transloco"
                [title]="'chat.composer.recordingCancel' | transloco"
              >
                <app-icon size="sm">close</app-icon>
              </button>
              <canvas #waveformCanvas class="recording-canvas" width="600" height="56"></canvas>
              <span class="recording-time" [class.near-cap]="recordingNearCap()">
                <span class="recording-dot"></span>
                {{ recordingDurationLabel() }}
              </span>
              @if (recordingNearCap()) {
                <span class="recording-cap-warning">{{ 'chat.composer.recordingCapWarning' | transloco }}</span>
              }
              <button
                type="button"
                class="recording-btn confirm"
                (click)="stopRecording()"
                [attr.aria-label]="'chat.composer.recordingStop' | transloco"
                [title]="'chat.composer.recordingStop' | transloco"
              >
                <app-icon size="sm">check</app-icon>
              </button>
            </div>
          } @else {
            @if (chat.rewindOutcomeUnknown()) {
              <div class="queued-actions">
                <span class="queued-note">{{ 'chat.rewind.outcomeUnknown' | transloco }}</span>
                <button type="button" class="queued-action"
                        [disabled]="chat.rewindInFlight()"
                        (click)="chat.refreshPendingRewindReceipt()">{{ 'chat.rewind.refreshReceipt' | transloco }}</button>
                <button type="button" class="queued-action"
                        [disabled]="chat.rewindInFlight()"
                        (click)="chat.retryPendingRewind()">{{ 'chat.rewind.retryExact' | transloco }}</button>
              </div>
            }
            @if (chat.rewindPrefill(); as prefill) {
              <button type="button" class="queued-action" (click)="insertRewindPrompt(prefill)">
                {{ 'chat.rewind.insertOriginalPrompt' | transloco }}
              </button>
            }
            <textarea
              #inputEl
              class="chat-input"
              data-testid="chat-composer"
              [(ngModel)]="inputText"
              (ngModelChange)="onInputChange($event)"
              (input)="autoResizeInput()"
              (keydown)="onKeydown($event)"
              (paste)="onPaste($event)"
              (focus)="inputFocused.set(true)"
              (blur)="inputFocused.set(false)"
              [placeholder]="inputPlaceholder()"
              [attr.aria-label]="'chat.input.defaultMobile' | transloco"
              [disabled]="!canCompose()"
              rows="1"
            ></textarea>
          }

          @if (!isRecording()) {
          <div class="composer-row">
            <!-- Attach button + popover menu -->
            <div class="attach-wrap">
              <button
                type="button"
                class="ctrl"
                [disabled]="!chat.isConnected() || chat.isParked()"
                [title]="'chat.composer.attach' | transloco"
                [class.active]="attachmentMenuOpen()"
                (click)="attachmentMenuOpen() ? closeAttachmentMenu() : openAttachmentMenu()"
              >
                <app-icon size="sm" class="ctrl-icon">attach_file</app-icon>
                <span class="ctrl-label">{{ 'chat.composer.attach' | transloco }}</span>
              </button>
              @if (attachmentMenuOpen()) {
                <div class="attach-menu" (click)="$event.stopPropagation()">
                  <button type="button" class="attach-menu-item" (click)="pickFile()">
                    <app-icon size="sm">folder_open</app-icon>
                    <span>{{ 'chat.composer.chooseFile' | transloco }}</span>
                  </button>
                  @if (hasCamera()) {
                    <button type="button" class="attach-menu-item" (click)="pickCamera()">
                      <app-icon size="sm">photo_camera</app-icon>
                      <span>{{ 'chat.composer.takePhoto' | transloco }}</span>
                    </button>
                  }
                </div>
                <div class="attach-menu-backdrop" (click)="closeAttachmentMenu()"></div>
              }
            </div>

            <!-- Direct camera shortcut on mobile devices -->
            @if (hasCamera() && isMobileDevice()) {
              <button
                type="button"
                class="ctrl"
                [disabled]="!chat.isConnected() || chat.isParked()"
                [title]="'chat.composer.takePhoto' | transloco"
                (click)="pickCamera()"
              >
                <app-icon size="sm" class="ctrl-icon">photo_camera</app-icon>
              </button>
            }

            <span class="spacer"></span>

            <!-- Action button: mic while the composer is empty, send once there is
                 something to send, stop/spinner while a turn is in flight.
                 pointerdown.preventDefault keeps the textarea focused through the
                 tap, so the on-screen keyboard doesn't reflow the whole column
                 mid-tap; on mobile send() then blurs deliberately, dismissing the
                 keyboard so the reply gets the reclaimed height. -->
            @if (micMode()) {
              <button
                type="button"
                class="send mic"
                [disabled]="!chat.isConnected() || chat.isParked() || isTranscribing() || sttUnavailable()"
                [title]="(sttUnavailable() ? 'chat.composer.sttNotConfigured' : 'chat.composer.recordVoice') | transloco"
                (pointerdown)="$event.preventDefault()"
                (click)="startRecording()"
              >
                <app-icon size="sm" class="action-icon">mic</app-icon>
              </button>
            } @else {
              <button
                type="button"
                class="send"
                data-testid="chat-send"
                [class.stop]="chat.isStreaming() && !chat.isInterrupting()"
                [class.interrupting]="chat.isInterrupting()"
                [class.pending]="isPendingSend()"
                [title]="(chat.isStreaming() ? 'chat.composer.stop' : 'chat.composer.send') | transloco"
                [attr.aria-label]="(chat.isStreaming() ? 'chat.composer.stop' : 'chat.composer.send') | transloco"
                (pointerdown)="$event.preventDefault()"
                (click)="chat.isStreaming() ? chat.interrupt() : send()"
                [disabled]="chat.isInterrupting() || (!chat.isStreaming() && !canSend())"
              >
                @if (isPendingSend() || chat.isInterrupting() || chat.isAwaitingTurn()) {
                  <span class="action-spinner"></span>
                } @else if (chat.isStreaming()) {
                  <app-icon size="sm" class="action-icon">stop</app-icon>
                } @else {
                  <app-icon size="sm" class="action-icon">arrow_upward</app-icon>
                }
              </button>
            }
          </div>
          }
        </div>

        <!-- Hidden file inputs -->
        <input
          #fileInput
          type="file"
          multiple
          accept="image/*,video/*,audio/*,.pdf,.doc,.docx,.txt,.md,.csv,.xls,.xlsx,.zip"
          (change)="onFilesSelected($event)"
          style="display: none;"
        />
        <input
          #cameraInput
          type="file"
          accept="image/*"
          capture="environment"
          (change)="onFilesSelected($event)"
          style="display: none;"
        />
      </div>

      <!-- Image preview dialog -->
      <app-dialog
        [open]="imagePreviewUrl() !== null"
        [title]="imagePreviewName()"
        size="lg"
        (closed)="closeImagePreview()"
      >
        @if (imagePreviewUrl(); as url) {
          <img [src]="url" [alt]="imagePreviewName()" class="image-preview-img" />
        }
      </app-dialog>

      <!-- /rewind target picker -->
      <app-dialog
        [open]="rewindPickerOpen()"
        (closed)="closeRewindPicker()"
        [title]="'chat.rewind.pickerTitle' | transloco"
        size="md">
        @if (rewindCandidates().length === 0) {
          <p class="rewind-picker-empty">{{ 'chat.rewind.pickerEmpty' | transloco }}</p>
        } @else {
          <div class="rewind-picker" (keydown)="onRewindPickerKeydown($event)">
            @if (rewindCandidates().length >= rewindFilterMin) {
              <input
                class="rewind-picker-filter"
                type="text"
                [placeholder]="'chat.rewind.pickerFilter' | transloco"
                [attr.aria-label]="'chat.rewind.pickerFilter' | transloco"
                (input)="rewindPickerQuery.set($any($event.target).value)" />
            } @else {
              <p class="rewind-picker-hint">{{ 'chat.rewind.pickerHint' | transloco }}</p>
            }
            @if (filteredRewindCandidates().length === 0) {
              <p class="rewind-picker-empty">{{ 'chat.rewind.pickerNoMatches' | transloco }}</p>
            } @else {
              <div class="rewind-picker-list">
                @for (turn of filteredRewindCandidates(); track turn.id) {
                  <button type="button" class="rewind-picker-item"
                          [title]="turn.content"
                          (click)="pickRewindTarget(turn)">
                    <span class="rewind-picker-time">{{ rewindStamp(turn.timestamp) }}</span>
                    <span class="rewind-picker-text">{{ turn.content }}</span>
                  </button>
                }
              </div>
            }
          </div>
        }
        <ng-container appDialogActions>
          <app-button variant="ghost" size="sm" (clicked)="closeRewindPicker()">
            {{ 'common.cancel' | transloco }}
          </app-button>
        </ng-container>
      </app-dialog>

      <!-- Rewind action dialog -->
      <app-dialog
        [open]="rewindTarget() !== null"
        (closed)="closeRewindSheet()"
        [title]="'chat.rewind.title' | transloco"
        size="md">
        <p class="rewind-quote">"{{ rewindQuote() }}"</p>
        <p class="rewind-target-meta">
          {{ rewindTargetStamp() }}
          @if (rewindHiddenCount() > 0) {
            <span> · {{ (rewindHiddenCount() === 1 ? 'chat.rewind.hidesOne' : 'chat.rewind.hidesMany') | transloco: {count: rewindHiddenCount()} }}</span>
          }
        </p>
        @if (chat.rewindPreviewLoading()) {
          <p class="rewind-caveat">{{ 'chat.rewind.checking' | transloco }}</p>
        }
        <div class="rewind-options">
          @if (chat.rewindModeAvailable('both')) {
            <button type="button" class="rewind-option"
                    [disabled]="chat.rewindInFlight()"
                    (click)="confirmRewind('both')">
              <app-icon size="sm" class="rewind-option-icon">history</app-icon>
              <span class="rewind-option-text">
                <span class="rewind-option-title">{{ 'chat.rewind.both' | transloco }}</span>
                <span class="rewind-option-desc">{{ 'chat.rewind.bothDesc' | transloco }}</span>
              </span>
            </button>
          }
          @if (chat.rewindModeAvailable('conversation')) {
            <button type="button" class="rewind-option"
                  [disabled]="chat.rewindInFlight() || chat.rewindPreviewLoading() || (chat.controlTransport('rewind') === 'rest' && !chat.rewindPreview()?.eligible)"
                  (click)="confirmRewind('conversation')">
              <app-icon size="sm" class="rewind-option-icon">chat_bubble</app-icon>
              <span class="rewind-option-text">
                <span class="rewind-option-title">{{ 'chat.rewind.conversation' | transloco }}</span>
                <span class="rewind-option-desc">{{ 'chat.rewind.conversationDesc' | transloco }}</span>
              </span>
            </button>
          }
          @if (chat.rewindModeAvailable('code')) {
            <button type="button" class="rewind-option"
                    [disabled]="chat.rewindInFlight()"
                    (click)="confirmRewind('code')">
              <app-icon size="sm" class="rewind-option-icon">folder</app-icon>
              <span class="rewind-option-text">
                <span class="rewind-option-title">{{ 'chat.rewind.code' | transloco }}</span>
                <span class="rewind-option-desc">{{ 'chat.rewind.codeDesc' | transloco }}</span>
              </span>
            </button>
          }
        </div>
        @if (chat.summarizeAvailable()) {
          <div class="rewind-options rewind-options-secondary">
          <button type="button" class="rewind-option"
                  (click)="confirmSummarizeUpTo()">
            <app-icon size="sm" class="rewind-option-icon">compress</app-icon>
            <span class="rewind-option-text">
              <span class="rewind-option-title">{{ 'chat.rewind.summarize' | transloco }}</span>
              <span class="rewind-option-desc">{{ 'chat.rewind.summarizeDesc' | transloco }}</span>
            </span>
          </button>
          </div>
        }
        <p class="rewind-caveat">{{ 'chat.rewind.refillHint' | transloco }}</p>
        @if (chat.controlTransport('rewind') === 'rest') {
          <p class="rewind-caveat">{{ 'chat.rewind.unchangedState' | transloco }}</p>
        }
        <p class="rewind-caveat">{{ 'chat.rewind.caveat' | transloco }}</p>
        <ng-container appDialogActions>
          <app-button variant="ghost" size="sm" (clicked)="closeRewindSheet()">
            {{ 'common.cancel' | transloco }}
          </app-button>
        </ng-container>
      </app-dialog>
    </div>
  `,
    styleUrls: ['./persistent-chat.component.scss'],
})
export class PersistentChatComponent implements OnInit, AfterViewChecked, OnDestroy {
    readonly canvasRequested = output<void>();
    /** An agent-written workspace path was activated in rendered Markdown.
     *  The host chat-page decides what a file can be shown in. */
    readonly workspaceFileRequested = output<string>();
    /** Open the session settings pane (host chat-page owns it). The optional
     *  payload names a section to focus — 'model' from the model/temp chips. */
    readonly settingsRequested = output<string | undefined>();
    readonly chat = inject(PersistentChatService);
    readonly viewport = inject(ViewportService);
    private readonly api = inject(ApiService);
    private readonly transloco = inject(TranslocoService);
    private readonly i18n = inject(I18nService);
    private readonly http = inject(HttpClient);
    private readonly fileHandling = inject(FileHandlingService);
    readonly chatPrefs = inject(ChatPreferencesService);

    /** `--chat-content-width` / `--chat-body-font-size` bound on `.chat-container`,
     *  derived from the per-device preferences (see readingWidthToCss/textSizeToCss). */
    readonly chatWidthValue = computed(() => readingWidthToCss(this.chatPrefs.readingWidth()));
    readonly chatTextSizeValue = computed(() => textSizeToCss(this.chatPrefs.textSize()));
    private readonly deviceCapabilities = inject(DeviceCapabilitiesService);
    readonly capabilities = inject(CapabilitiesService);
    readonly voiceCaps = inject(VoiceCapabilitiesService);
    private readonly voiceRecording = inject(VoiceRecordingService);
    private readonly router = inject(Router);
    private readonly toast = inject(AppToastService);
    private readonly errors = inject(ErrorMessageService);
    private readonly sessionList = inject(SessionListService);
    private readonly injector = inject(Injector);
    private readonly destroyRef = inject(DestroyRef);

    /**
     * The running-command card to show on (re)attach (or null). Surfaces the
     * agent's in-flight tool call when it isn't already visible in a turn — see
     * pickRunningCommandCard.
     */
    readonly runningCommandCard = computed(() =>
        pickRunningCommandCard(this.chat.runningTool(), this.chat.turns()),
    );

    /** The workspace-upgrade offer/provisioning card, or null. */
    readonly workspaceOfferCard = computed(() =>
        pickWorkspaceOfferCard(
            this.chat.pendingWorkspaceOffer(),
            this.chat.workspaceUpgradeInProgress(),
            this.chat.continueAfterUpgrade(),
        ),
    );

    // --- Live compaction progress (knowledge-base/knowledge/features/context_summarization_rework.md S3)
    /** 1s tick driving the elapsed timer; interval runs only mid-compaction. */
    /** Escalation tier + m:ss label for the queued-turn bubble. */
    readonly queuedTier = computed(() => queueWaitTier(this.chat.awaitingElapsedMs()));
    readonly queuedWaitLabel = computed(() => formatQueueWait(this.chat.awaitingElapsedMs()));
    /** Reason line under the parked bubble, keyed by run_queue.park_reason. */
    readonly parkedReasonKey = computed(() => queueParkReasonKey(this.chat.queueState()?.park_reason));
    readonly retryingParked = signal(false);

    async retryParked(): Promise<void> {
        if (this.retryingParked()) return;
        this.retryingParked.set(true);
        try {
            await this.chat.retryParked();
        } finally {
            this.retryingParked.set(false);
        }
    }

    private readonly compactionNow = signal(Date.now());
    private compactionTimer: ReturnType<typeof setInterval> | null = null;

    readonly compactionElapsed = computed(() => {
        const comp = this.chat.compaction();
        if (!comp) return '0:00';
        const secs = Math.max(0, Math.floor((this.compactionNow() - comp.startedAt) / 1000));
        const m = Math.floor(secs / 60);
        return `${m}:${(secs % 60).toString().padStart(2, '0')}`;
    });

    /** One segment per pass for small counts; [] switches the template to the
     * continuous bar — a 9000-pass compaction must render fine too. */
    readonly compactionSegments = computed(() => {
        const comp = this.chat.compaction();
        if (!comp || comp.nPasses < 1 || comp.nPasses > 20) return [] as number[];
        return Array.from({length: comp.nPasses}, (_, i) => i + 1);
    });

    formatTokens(n: number): string {
        return n >= 1000 ? `${(n / 1000).toFixed(1)}k` : `${n}`;
    }

    /** Context fill % for the usage panel gauge (null until usage known).
     *  Anchored on the auto-compaction trigger (``compactionThresholdTokens``,
     *  the effective working-context ceiling), not the raw model window — so
     *  100% ≈ "a compaction is about to fire" rather than an arbitrary fraction
     *  of a window the agent never lets fill. Falls back to the model window if
     *  the agent hasn't reported the threshold yet (older backend). */
    readonly usageCtxPct = computed(() => {
        const u = this.chat.currentUsage();
        if (!u || u.inputTokens == null) return null;
        const denom = u.compactionThresholdTokens || u.ctxLimitTokens;
        if (!denom) return null;
        return Math.min(100, Math.round((100 * u.inputTokens) / denom));
    });

    /** Context-fill threshold level driving the gauge colour ramp
     *  (ok → warn → danger). Because the pct is anchored on the compaction
     *  trigger, ``danger`` (≥90%) literally means a compaction is imminent and
     *  ``warn`` (≥75%) that it's approaching. A null pct reads as 'ok'. */
    readonly usageCtxLevel = computed<'ok' | 'warn' | 'danger'>(() => {
        const pct = this.usageCtxPct();
        if (pct == null) return 'ok';
        if (pct >= 90) return 'danger';
        if (pct >= 75) return 'warn';
        return 'ok';
    });

    @ViewChild('messagesContainer') messagesContainer!: ElementRef<HTMLDivElement>;
    /**
     * The scroll pin's second observation target. Signal query because it's new
     * code; `messagesContainer` stays a decorator query because ~10 call sites
     * read it and migrating them is out of scope. `.required` is safe — neither
     * element sits inside an `@if`.
     */
    private readonly messagesInner = viewChild.required<ElementRef<HTMLDivElement>>('messagesInner');
    /** Header + its action row — measured to decide when the actions fold into
     *  the overflow menu (see the header-fold ResizeObserver in the ctor). */
    private readonly chatHeaderEl = viewChild.required<ElementRef<HTMLDivElement>>('chatHeaderEl');
    private readonly headerActionsEl = viewChild.required<ElementRef<HTMLDivElement>>('headerActionsEl');
    @ViewChild('inputEl') inputEl!: ElementRef<HTMLTextAreaElement>;
    @ViewChild('fileInput') fileInput?: ElementRef<HTMLInputElement>;
    @ViewChild('cameraInput') cameraInput?: ElementRef<HTMLInputElement>;
    @ViewChild('waveformCanvas') waveformCanvas?: ElementRef<HTMLCanvasElement>;

    inputText = '';
    private composerDraftRevision = 0;

    // Settings panel
    readonly showViewMenu = signal(false);
    readonly showCitations = signal(false);
    readonly showSshPanel = signal(false);

    /** Disconnect is a ~5s round trip (DELETE + agent/workspace teardown). The
     *  header button spins for it so the click reads as accepted; without it
     *  the whole wait looks like a dead button. */
    readonly isDisconnecting = signal(false);

    /** Bare hostname for `srw-ssh-proxy --stdio <apiHost>` (ssh-config.ts's
     *  HOST_PATTERN admits no scheme, path or port). `environment.apiUrl`
     *  carries the `/api` suffix and, in local/k3d dev, a scheme and port —
     *  only the hostname survives here. Computed once: the orchestrator's
     *  public host does not change without a page reload. */
    readonly sshApiHost = (() => {
        try {
            return new URL(environment.apiUrl).hostname;
        } catch {
            return '';
        }
    })();

    /** `SshGatewayInfo.hostname` (ssh.srw.works), or '' while loading / when
     *  the deployment has no gateway. Pulled out of the template because
     *  Angular's template type-checker mis-narrows the chained optional
     *  member + `noUncheckedIndexedAccess` index access below when written
     *  inline (TS2532 on a fully-guarded expression). */
    readonly sshGatewayHostname = computed(() => this.capabilities.sshGateway()?.hostname ?? '');

    /** First published host key's fingerprint, for first-connect
     *  verification. Only ever one entry in practice — the gateway rejects
     *  any host key that isn't Ed25519 (services/ssh_gateway_config.py) —
     *  but this still degrades to '' rather than assume the array is
     *  non-empty. */
    readonly sshHostKeyFingerprint = computed(() => {
        const gateway = this.capabilities.sshGateway();
        if (!gateway || gateway.host_keys.length === 0) return '';
        return gateway.host_keys[0]?.fingerprint ?? '';
    });

    /** SSH button gate: a deployed gateway AND a workspace it can reach —
     *  see {@link sshWorkspaceReachable} for what counts as reachable. */
    readonly sshButtonVisible = computed(
        () => !!this.capabilities.sshGateway() && sshWorkspaceReachable(this.ideStatus()),
    );

    /**
     * Fold the header's secondary actions into the `⋮` overflow menu.
     *
     * Driven by the header's OWN width, not the viewport's: the chat pane shares
     * its row with the canvas/settings pane, so a 2560px desktop can hand this
     * header 500px. Keyed off `viewport.isMobile()` the actions simply ran past
     * the pane edge and were clipped by the split area — the buttons neither
     * moved nor shrank, they just stopped existing. See
     * knowledge-history/done/canvas_settings_pane_clips_chat_header_actions.md.
     */
    readonly headerCompact = computed(
        () => this.viewport.isMobile() || this.headerActionsOverflow(),
    );
    /** Measured half of {@link headerCompact} — set by the header ResizeObserver. */
    private readonly headerActionsOverflow = signal(false);
    /**
     * Natural width of the action row, remembered from the last render where it
     * was NOT folded. Folding shrinks the row, which would otherwise "prove"
     * there is room and unfold it again — this is the hysteresis anchor.
     */
    private headerActionsNaturalWidth = 0;

    // Input state
    readonly inputFocused = signal(false);

    // Composer attachments — device capabilities, recording, image preview.
    readonly hasCamera = signal(false);
    readonly hasAudioInput = signal(false);
    readonly isMobileDevice = signal(false);
    readonly attachmentMenuOpen = signal(false);
    readonly isRecording = signal(false);
    readonly recordingDuration = signal(0);
    readonly isTranscribing = signal(false);
    // Hard cap on a single dictation (20 min). Generous enough for the "10+ min
    // voice message" case, while bounding memory + staying well under the 25 MB
    // backend cap (opus ≈ 0.3 MB/min ⇒ ~6 MB). The recording service is handed
    // this but doesn't enforce it, so we auto-stop here (below).
    private readonly maxRecordingSeconds = 1200;
    private capStopTriggered = false;
    /** True in the last minute before the cap → show a "stops soon" warning. */
    readonly recordingNearCap = computed(
        () =>
            this.isRecording() &&
            this.recordingDuration() >= this.maxRecordingSeconds - 60,
    );
    /** Recording elapsed as m:ss (raw seconds reads badly near the 20-min cap). */
    readonly recordingDurationLabel = computed(() => {
        const s = this.recordingDuration();
        return `${Math.floor(s / 60)}:${(s % 60).toString().padStart(2, '0')}`;
    });
    readonly imagePreviewUrl = signal<string | null>(null);
    readonly imagePreviewName = signal<string>('');
    // Drag-and-drop overlay state. dragEnterCount handles the
    // dragenter/dragleave-on-child-element quirk: the leave fires every
    // time the cursor crosses any nested element border, so we only hide
    // the overlay when the counter returns to zero.
    readonly isDragOver = signal(false);
    private dragEnterCount = 0;

    private capabilitiesSub?: Subscription;
    private recordingStateSub?: Subscription;

    // Dictation availability for the mic button. Only *positively-known*
    // unavailability disables it (fail-open: null/true ⇒ leave it enabled).
    // Read-aloud availability + playback now live in <app-read-aloud>.
    readonly sttUnavailable = computed(() => this.voiceCaps.stt() === false);

    // Slash command autocomplete
    readonly showSlashMenu = signal(false);
    readonly slashSelectedIndex = signal(0);
    private readonly slashQuery = signal('');
    readonly filteredCommands = computed(() =>
        SLASH_COMMANDS.filter(c =>
            c.command.startsWith(this.slashQuery()) &&
            (c.command !== '/rewind' || this.chat.rewindModeAvailable('conversation'))));

    // Empty-state suggestions (loaded once per mount; the whole set renders, nothing is picked)
    private readonly pickedSuggestions = signal<Suggestion[]>([]);
    readonly displayedSuggestions = computed<DisplayedSuggestion[]>(() => {
        const lang = this.i18n.activeLang();
        return this.pickedSuggestions().map(s => ({
            icon: s.icon,
            text: lang === 'de-DE' ? (s.de || s.en) : s.en,
        }));
    });

    formatEndedAt(value: string | null): string {
        if (!value) return '';
        const lang = this.i18n.activeLang();
        try {
            return new Intl.DateTimeFormat(lang, {
                dateStyle: 'long',
                timeStyle: 'short',
            }).format(new Date(value));
        } catch {
            return value;
        }
    }

    // Startup step list — drives the provisioning card.
    // Order maps to the phases the orchestrator emits via the `status` WS message.
    private readonly STARTUP_PHASE_ORDER = ['creating', 'provisioning', 'booting', 'connecting'] as const;

    // Per-phase timing. Plain mutable maps + a revision signal so the computed
    // re-runs on transitions without forming a self-referential signal loop.
    private phaseStarts: Record<string, number> = {};
    private phaseDurations: Record<string, number> = {};
    private readonly phaseRevision = signal(0);
    private readonly nowTick = signal<number>(Date.now());
    private startupTickInterval: ReturnType<typeof setInterval> | null = null;
    private prevActivePhase: string | null = null;
    private prevReadyForTiming = false;
    private prevThreadIdForTiming: string | null = null;

    readonly startupSteps = computed(() => {
        const order = this.STARTUP_PHASE_ORDER;
        const phase = this.chat.startupPhase();
        const isResuming = this.chat.turns().length > 0;
        // Subscribe to phase tracking changes and to the live tick.
        this.phaseRevision();
        const now = this.nowTick();
        let activeIdx: number;
        if (this.chat.isCreating()) {
            activeIdx = 0;
        } else if (phase && (order as readonly string[]).includes(phase)) {
            activeIdx = (order as readonly string[]).indexOf(phase);
        } else {
            // No live phase signal. Phases arrive in two batches: 'creating'
            // is set client-side and cleared in disconnect() before the
            // server-emitted 'provisioning' status frame arrives over the
            // control WS. During that gap, the previous fallback (`isResuming
            // ? 1 : 0`) regressed step 0 back to 'active', wiping the just-
            // recorded "2.0s" with a fresh spinner. Use the completed-count
            // as the floor so a step that finished stays done.
            const completedCount = order.filter(k => this.phaseDurations[k] != null).length;
            activeIdx = Math.max(completedCount, isResuming ? 1 : 0);
        }
        const isVm = this.chat.isVmSession();
        return order.map((key, idx) => {
            const state: 'done' | 'active' | 'todo' =
                idx < activeIdx ? 'done' : (idx === activeIdx ? 'active' : 'todo');
            let elapsedMs: number | null = null;
            if (state === 'done') {
                elapsedMs = this.phaseDurations[key] ?? null;
            } else if (state === 'active' && this.phaseStarts[key] != null) {
                elapsedMs = Math.max(0, now - this.phaseStarts[key]);
            }
            // A VM boot is the one multi-minute step; give it distinct copy so
            // the user knows the wait is expected, not a hang.
            const labelKey =
                key === 'booting' && isVm
                    ? 'chat.startup.steps.bootingVm'
                    : 'chat.startup.steps.' + key;
            return { key, state, elapsedMs, labelKey };
        });
    });

    /** The startup step to surface in the slim banner (active, or last as a fallback). */
    readonly currentStartupStep = computed(() => pickCurrentStartupStep(this.startupSteps()));

    /**
     * Show the slim startup banner once a turn exists while still starting —
     * mutually exclusive with the @empty centered card, so the card never
     * floats over the message list.
     */
    readonly startupBannerVisible = computed(() =>
        isStartupBannerVisible(this.chat.isStartingSession(), this.chat.turns().length),
    );

    /**
     * Whether the composer accepts input: during startup (type + queue +
     * flush on ready), while connected, and in the landing draft (type first,
     * session created on send); false during a mid-session reconnect and
     * while the session's unit is parked.
     */
    readonly canCompose = computed(() =>
        canComposeDuringSession(
            this.chat.isConnected(),
            this.chat.isStartingSession(),
            this.chat.isDraftSession(),
            this.chat.threadStatus() === 'ended' || this.chat.threadStatus() === 'suspended',
            this.chat.isResuming(),
            this.chat.isParked(),
        ),
    );

    stepIcon(state: 'done' | 'active' | 'todo'): string {
        if (state === 'done') return 'check_circle';
        if (state === 'active') return 'progress_activity';
        return 'radio_button_unchecked';
    }

    formatElapsed(ms: number | null): string {
        if (ms == null) return '—';
        const s = Math.max(0, ms) / 1000;
        if (s < 10) return `${s.toFixed(1)}s`;
        if (s < 60) return `${Math.round(s)}s`;
        const m = Math.floor(s / 60);
        const rs = Math.round(s % 60);
        return `${m}m${rs.toString().padStart(2, '0')}s`;
    }

    // IDE status
    readonly ideStatus = signal<IdeSessionStatus | null>(null);
    private idePollingTimer: ReturnType<typeof setInterval> | null = null;
    private idePollingAttempts = 0;

    private autoScroll = true;
    private lastSeenMessageCount = 0;
    /** Suppresses the scroll handler while restoring position after prepending older turns. */
    private isRestoringScroll = false;

    constructor() {
        // Start/stop IDE polling when connection state changes
        effect(() => {
            const connected = this.chat.isConnected();
            const threadId = this.chat.threadId();
            if (connected && threadId) {
                this.startIdePolling(threadId);
            } else {
                this.stopIdePolling();
            }
        });

        // Restore a persisted composer draft when a thread loads (e.g. after an
        // auth-redirect reload). Empty-guarded so it never clobbers text the
        // user is already typing; onInputChange writes the draft on every
        // keystroke and send() clears it once the message is in flight.
        effect(() => {
            const threadId = this.chat.threadId();
            if (!threadId || this.inputText.trim()) return;
            const draft = loadDraft(threadId);
            if (draft) this.inputText = draft;
        });

        // Rewind hands the un-sent prompt back for edit-and-resend. Plain
        // ngModel field: assign + saveDraft by hand (no ngModelChange fires),
        // same trap denyOffer documents.
        effect(() => {
            const prefill = this.chat.rewindPrefill();
            if (
                prefill === null ||
                this.inputText !== prefill.draftSnapshot ||
                this.composerDraftRevision !== prefill.draftRevision
            ) return;
            this.inputText = prefill.prompt;
            this.composerDraftRevision++;
            saveDraft(this.chat.threadId(), this.inputText);
            this.chat.consumeRewindPrefill(prefill.clientRequestId);
            setTimeout(() => {
                this.inputEl?.nativeElement?.focus();
                this.autoResizeInput();
            });
        });

        // Elapsed-timer tick, only while a compaction is in flight.
        effect(() => {
            const active = this.chat.compaction() !== null;
            if (active && this.compactionTimer === null) {
                this.compactionNow.set(Date.now());
                this.compactionTimer = setInterval(
                    () => this.compactionNow.set(Date.now()),
                    1000,
                );
            } else if (!active && this.compactionTimer !== null) {
                clearInterval(this.compactionTimer);
                this.compactionTimer = null;
            }
        });

        // Load the empty-state suggestions. The set is deliberately NOT shuffled:
        // picking 4 of 8 at random per mount meant a chip seen once could not be
        // found again, which read as the list being broken.
        this.http.get<Suggestion[]>('assets/suggestions.json').subscribe({
            next: (list) => this.pickedSuggestions.set(list ?? []),
            error: () => this.pickedSuggestions.set([]),
        });

        // ===== The scroll pin =====
        //
        // ONE invariant: if the user is following the bottom, keep the bottom in
        // view — no matter what changed the height. This replaced six hand-added
        // hooks that each patched one *known cause* (turn appended, attachment
        // chip added, keyboard opened, ...). That list could never be complete:
        // the resume card was 1 of ~18 untracked height changes, and every new
        // bottom element was a latent bug until someone added hook #7. Observe
        // the effect, don't enumerate the causes.
        //
        // Full rationale, measurements and the rejected alternatives (scroll
        // anchoring, column-reverse, scroll-snap, CDK, afterRenderEffect, a
        // MutationObserver) live in
        // knowledge-base/knowledge/issues/cockpit_session_scroll_pin_misses_late_height_changes.md.
        afterNextRender(() => {
            const container = this.messagesContainer.nativeElement;
            const inner = this.messagesInner().nativeElement;

            // One observer, two targets — the asymmetry is the whole point. RO
            // reports an element's OWN box, so:
            //   inner     -> content growth (turns, streaming deltas, <img>
            //                decode, webfont swap, code-block collapse, the
            //                threadStatus resume card)
            //   container -> viewport shrink (composer autosize, Android
            //                keyboard, sibling banners) — content growth NEVER
            //                changes its box, it's flex:1.
            // Observing only `container` would never fire on a new message.
            // Both changing in one frame -> one callback, two entries -> one pin.
            const ro = new ResizeObserver(() => {
                // Pure DOM write: no signal reads, so there is no dependency
                // list to forget — that is the entire point. Runs after layout,
                // before paint (HTML "update the rendering" step 16; rAF is step
                // 14, paint is step 22), so NEVER defer this into rAF or
                // setTimeout: a rAF scheduled from here lands NEXT frame and the
                // current one paints 160px stale. That IS the historic
                // up-then-down jump, and it's what the deleted setTimeout(0)
                // hooks were doing.
                if (!shouldPin(this.autoScroll, this.isRestoringScroll)) return;
                container.scrollTop = pinTarget(container.scrollHeight, container.clientHeight);
            });

            ro.observe(inner);
            ro.observe(container);

            // The dangerous race is the opposite of the obvious one: the user
            // starts scrolling up, an RO tick lands before their scroll event,
            // autoScroll is still true, the pin yanks them back — and their
            // scroll event then computes at-bottom, so autoScroll stays true and
            // they physically cannot read back during streaming. RO fires on
            // every delta while streaming, so this gets *more* likely, not less.
            // Wheel/touch is the only user-intent signal a layout shift cannot
            // forge, which is why the escape can't be expressed via scroll events.
            const onWheel = ({deltaY}: WheelEvent) => {
                if (deltaY < 0 && container.scrollHeight > container.clientHeight) {
                    this.autoScroll = false;
                }
            };
            // Touch mirror of the wheel escape: a finger dragging DOWN scrolls
            // the content UP (away from the bottom). Same intent signal.
            let lastTouchY: number | null = null;
            const onTouchStart = (e: TouchEvent) => {
                lastTouchY = e.touches[0]?.clientY ?? null;
            };
            const onTouchMove = (e: TouchEvent) => {
                const y = e.touches[0]?.clientY;
                if (y == null || lastTouchY == null) return;
                if (y > lastTouchY && container.scrollHeight > container.clientHeight) {
                    this.autoScroll = false;
                }
                lastTouchY = y;
            };
            container.addEventListener('wheel', onWheel, {passive: true});
            container.addEventListener('touchstart', onTouchStart, {passive: true});
            container.addEventListener('touchmove', onTouchMove, {passive: true});

            this.destroyRef.onDestroy(() => {
                ro.disconnect();
                container.removeEventListener('wheel', onWheel);
                container.removeEventListener('touchstart', onTouchStart);
                container.removeEventListener('touchmove', onTouchMove);
            });
        });

        // ===== The header fold =====
        //
        // Same shape as the scroll pin: observe the effect, don't enumerate the
        // causes. The header narrows for reasons this component can't see — the
        // canvas/settings pane opening, the split gutter being dragged, the
        // sidebar collapsing, the window resizing — so measure the box instead
        // of subscribing to each trigger.
        //
        // Two targets again, and again asymmetric:
        //   header  -> the space available (pane resize, gutter drag)
        //   actions -> the space demanded (the IDE button appearing once the
        //              workspace is up, citations arriving, i18n swap). The
        //              header's own box NEVER changes when a button appears, so
        //              observing only it would fold too late — or not at all.
        afterNextRender(() => {
            const header = this.chatHeaderEl().nativeElement;
            const actions = this.headerActionsEl().nativeElement;

            const evaluate = () => {
                const compact = this.headerCompact();
                // `.header-right` is flex: 0 0 auto, so its box is its natural
                // width even while it overflows — no hidden probe render needed.
                // Only trust it while unfolded: the folded row is ~150px and
                // would "prove" there is room, then unfold, then overflow again.
                if (!compact) this.headerActionsNaturalWidth = actions.offsetWidth;

                const cs = getComputedStyle(header);
                const inner =
                    header.clientWidth -
                    (parseFloat(cs.paddingLeft) || 0) -
                    (parseFloat(cs.paddingRight) || 0);
                this.headerActionsOverflow.set(
                    shouldFoldHeaderActions(inner, this.headerActionsNaturalWidth, compact),
                );
            };

            const ro = new ResizeObserver(evaluate);
            ro.observe(header);
            ro.observe(actions);
            this.destroyRef.onDestroy(() => ro.disconnect());
        });

        // The placeholder renders inside the textarea's scroll area, so a
        // state swap to a longer string ("Type your message while the session
        // starts...") overflows the empty 56px box — and with the app's styled
        // scrollbars that shows a permanent track. Re-run the autosize when
        // the placeholder changes so the box grows to fit it.
        effect(() => {
            this.inputPlaceholder();
            queueMicrotask(() => this.autoResizeInput());
        });

        // Track new messages that arrive while the user has scrolled up.
        // Drives the "Jump to latest · N new" pill (F5).
        effect(() => {
            const len = this.chat.turns().length;
            const away = this.scrolledAway();
            if (len < this.lastSeenMessageCount) {
                // Thread switch or messages cleared — start fresh.
                this.newMessageCount.set(0);
            } else if (away && len > this.lastSeenMessageCount) {
                const delta = len - this.lastSeenMessageCount;
                this.newMessageCount.update(n => n + delta);
                this.chat.growWindow(delta); // anchor the visible top while scrolled away
            } else if (!away) {
                this.newMessageCount.set(0);
            }
            this.lastSeenMessageCount = len;
        });

        // Track startup phase transitions to record per-step durations.
        effect(() => {
            const phase = this.chat.startupPhase();
            const isCreating = this.chat.isCreating();
            const ready = this.chat.sessionReady();
            const tid = this.chat.threadId();
            const order = this.STARTUP_PHASE_ORDER as readonly string[];

            // Reset when switching to a different thread, or when sessionReady
            // drops back to false. The null → realId transition during new-thread
            // creation must NOT reset — that would wipe the 'creating' phase
            // timing recorded while threadId was still null.
            const threadChanged = this.prevThreadIdForTiming != null && tid !== this.prevThreadIdForTiming;
            if (threadChanged || (this.prevReadyForTiming && !ready)) {
                this.phaseStarts = {};
                this.phaseDurations = {};
                this.prevActivePhase = null;
                this.phaseRevision.update(v => v + 1);
            }
            this.prevThreadIdForTiming = tid;
            this.prevReadyForTiming = ready;

            // Determine effective active phase (mirrors startupSteps logic, but
            // we only record timing once we have a real signal — not during the
            // brief null gap on resume).
            let active: string | null = null;
            if (!ready) {
                if (isCreating) active = 'creating';
                else if (phase && order.includes(phase)) active = phase;
            }

            if (active !== this.prevActivePhase) {
                const now = Date.now();
                if (this.prevActivePhase != null) {
                    const start = this.phaseStarts[this.prevActivePhase];
                    if (start != null) {
                        this.phaseDurations[this.prevActivePhase] = now - start;
                    }
                }
                if (active && this.phaseStarts[active] == null) {
                    this.phaseStarts[active] = now;
                }
                this.prevActivePhase = active;
                this.phaseRevision.update(v => v + 1);
            }

            // Tick every second while a phase is active so the live elapsed
            // time on the active row updates without manual refresh.
            if (active && !this.startupTickInterval) {
                this.nowTick.set(Date.now());
                this.startupTickInterval = setInterval(() => this.nowTick.set(Date.now()), 1000);
            } else if (!active && this.startupTickInterval) {
                clearInterval(this.startupTickInterval);
                this.startupTickInterval = null;
            }
        });
    }

    readonly completedTaskCount = computed(() =>
        this.chat.tasks().filter(t => t.status === 'completed').length
    );

    readonly tasksCollapsed = signal(true);

    readonly nextPendingTask = computed(() => {
        const tasks = this.chat.tasks();
        return tasks.filter(t => t.status === 'pending' || t.status === 'in_progress').pop() ?? null;
    });

    toggleTasksCollapsed(): void {
        this.tasksCollapsed.update(v => !v);
    }

    readonly connectionClass = computed(() => this.chat.connectionState());
    readonly connectionLabel = computed(() => {
        // Track language changes so the label re-translates when i18n switches.
        this.i18n.activeLang();
        const state = this.chat.connectionState();
        const key =
            state === 'connected' ? 'chat.connection.connected'
                : state === 'connecting' ? 'chat.connection.connecting'
                    : state === 'error' ? 'chat.connection.error'
                        : 'chat.connection.disconnected';
        return this.transloco.translate(key);
    });

    readonly isShowingReconnectBanner = computed(() =>
        this.chat.connectionState() === 'disconnected'
        && this.chat.threadStatus() === 'active'
        && this.chat.sessionReady()
        && this.chat.turns().length > 0,
    );

    readonly scrolledAway = signal(false);
    readonly newMessageCount = signal(0);
    readonly showJumpToLatest = computed(
        () => this.scrolledAway() && this.newMessageCount() > 0,
    );

    readonly inputPlaceholder = computed(() => {
        // Track language changes so placeholder re-translates when i18n switches.
        this.i18n.activeLang();
        if (this.chat.isDraftSession()) {
            return this.transloco.translate(
                this.viewport.isMobile() ? 'chat.input.defaultMobile' : 'chat.input.default',
            );
        }
        if (this.isShowingReconnectBanner()) return this.transloco.translate('chat.input.reconnecting');
        // Sending has a side effect here (it brings the agent back), so say so
        // rather than letting the default "Enter to send" imply it's free.
        if (this.chat.isResuming()) return this.transloco.translate('chat.input.resuming');
        if (this.chat.threadStatus() === 'ending') {
            return this.transloco.translate('chat.input.ending');
        }
        if (this.chat.threadStatus() === 'ended') {
            return this.transloco.translate('chat.input.endedSendResumes');
        }
        if (this.chat.threadStatus() === 'suspended') {
            return this.transloco.translate('chat.input.suspendedSendResumes');
        }
        // A parked unit needs a retry — say so, not "waiting". A park an owner
        // may NOT revive (claim-loss hold, stop markers, a non-retryable
        // reason) renders no Retry button, so it must not name a retry either:
        // only an operator's admin unpark clears it. Ahead of the starting/
        // connecting copy — a reloaded parked session is still "starting",
        // since a park is no readiness evidence — because the composer is
        // closed while parked (canComposeDuringSession) and this line says why.
        if (this.chat.isParked()) {
            return this.transloco.translate(
                this.chat.queueState()?.retryable ? 'chat.input.parked' : 'chat.input.parkedBlocked',
            );
        }
        if (this.chat.isStartingSession()) return this.transloco.translate('chat.input.sessionStarting');
        if (!this.chat.isConnected()) return this.transloco.translate('chat.input.connect');
        if (this.chat.isInterrupting()) return this.transloco.translate('chat.input.stopping');
        if (this.chat.isStreaming()) return this.transloco.translate('chat.input.working');
        // isAwaitingTurn: the send is accepted but no agent has picked it up
        // yet — say "waiting", not "working"; the queued bubble carries the
        // escalation copy.
        if (this.chat.isAwaitingTurn()) return this.transloco.translate('chat.input.queued');
        // Mobile keyboards send newline on Enter, so the desktop key hints are wrong there.
        return this.transloco.translate(
            this.viewport.isMobile() ? 'chat.input.defaultMobile' : 'chat.input.default',
        );
    });

    /** True while sends are queued — waiting for readiness, flushing, or mid
     *  upload (an item stays in the outbox until every attachment resolves and
     *  the POST is accepted) — drives the send-button spinner. */
    readonly isPendingSend = computed(() => this.chat.outbox().length > 0);

    // Note: queueing is now supported, so canSend no longer blocks on a pending
    // send — the user can line up a second message while the first is in flight.
    // A method, NOT a computed: `inputText` is a plain ngModel field, so a
    // computed only re-evaluated when an unrelated signal dep happened to
    // change — on an otherwise-idle session the send button stayed disabled
    // while typing (Enter still worked; send() checks the field directly).
    // The (input)/(ngModelChange) bindings schedule CD, so a method re-reads
    // the field on every keystroke.
    canSend(): boolean {
        if (
            this.chat.isDraftSession() &&
            (this.chat.draftDefaultsLoading() || this.chat.draftDefaultsError() ||
                this.chat.draftDatasourceIds() === null)
        ) return false;
        return canSendMessage(
            this.canCompose(),
            this.inputText,
            this.chat.pendingAttachments().length,
        );
    }

    /** Empty composer → the action button offers dictation (method, not
     *  computed, for the same `inputText` reason as canSend). */
    micMode(): boolean {
        return isMicMode(
            this.hasAudioInput(),
            this.chat.isStreaming() ||
                this.chat.isInterrupting() ||
                this.isPendingSend() ||
                this.chat.isAwaitingTurn(),
            this.inputText,
            this.chat.pendingAttachments().length,
        );
    }

    ngOnInit(): void {
        this.capabilitiesSub = this.deviceCapabilities.getCapabilities().subscribe((caps) => {
            this.hasCamera.set(caps.hasCamera);
            this.hasAudioInput.set(caps.hasAudioInput);
            this.isMobileDevice.set(caps.isMobile);
        });
        this.recordingStateSub = this.voiceRecording.getRecordingState().subscribe((state) => {
            this.isRecording.set(state.isRecording);
            this.recordingDuration.set(state.duration);
            // Enforce the cap the service is handed but doesn't itself apply:
            // stop once at the limit so a long dictation ends cleanly (and stays
            // under the 25 MB backend cap) instead of growing unbounded.
            if (
                state.isRecording &&
                state.duration >= this.maxRecordingSeconds &&
                !this.capStopTriggered
            ) {
                this.capStopTriggered = true;
                void this.stopRecording();
            }
            if (!state.isRecording) this.capStopTriggered = false;
        });
    }

    ngAfterViewChecked(): void {
        this.collapseCodeBlocks();
        this.addCopyButtons();
    }

    ngOnDestroy(): void {
        // Don't disconnect — keep session alive across navigation
        this.stopIdePolling();
        if (this.startupTickInterval) {
            clearInterval(this.startupTickInterval);
            this.startupTickInterval = null;
        }
        if (this.compactionTimer) {
            clearInterval(this.compactionTimer);
            this.compactionTimer = null;
        }
        this.capabilitiesSub?.unsubscribe();
        this.recordingStateSub?.unsubscribe();
        if (this.isRecording()) {
            this.voiceRecording.cancelRecording();
        }
    }

    autoResizeInput(): void {
        const el = this.inputEl?.nativeElement;
        if (!el) return;
        el.style.height = 'auto';
        el.style.height = el.scrollHeight + 'px';
        // The composer and the message list are flex siblings, so the `height:auto`
        // reset above transiently enlarges .messages, and the browser clamps its
        // scrollTop up by ~one line on the shrunken scrollport — then never restores
        // it when the max grows back. Re-pin in the SAME frame, synchronously, so the
        // clamp and the re-pin never paint separately; a deferred re-pin is what made
        // the conversation visibly jump up-then-down on every keystroke.
        //
        // The ResizeObserver structurally CANNOT replace this one, which is why it
        // survived the cull: the enlargement is transient within a single task (the
        // `scrollHeight` read forces layout, then the next line restores the height),
        // so .messages ends the task at the size RO last reported. RO fires on
        // observed *change*; it never sees a transient. Only re-pin when the user was
        // already following the bottom.
        if (shouldPin(this.autoScroll, this.isRestoringScroll)) {
            this.scrollToBottom();
        }
    }

    send(): void {
        if (!this.canSend()) return;
        const text = this.inputText.trim();
        if (!text && this.chat.pendingAttachments().length === 0) return;

        const threadId = this.chat.threadId();
        this.showSlashMenu.set(false);
        // /rewind is pure UI — open the target picker instead of handing the
        // text to the service, which would send it to the agent as chat.
        if (isRewindCommand(text)) {
            // Not offered in the slash menu either, but a typed-out /rewind
            // must be refused out loud rather than vanish without a word.
            if (!this.chat.rewindModeAvailable('conversation')) {
                this.chat.error.set(this.transloco.translate('chat.rewind.unavailable'));
                return;
            }
            this.inputText = '';
            clearDraft(threadId);
            this.openRewindPicker();
            return;
        }
        // Mobile: dismiss the on-screen keyboard now the message is on its way,
        // so the reply renders into the reclaimed height. The keyboard collapse
        // grows .messages, which the ResizeObserver catches and re-pins
        // (autoScroll is set below). Desktop keeps focus for rapid follow-up.
        if (this.isMobileDevice()) {
            this.inputEl?.nativeElement?.blur();
        }
        // Clear textarea immediately — sendMessage is async because of uploads.
        this.inputText = '';
        // Drop the persisted draft now the message is in flight, so a reload
        // can't re-surface a message that was actually sent.
        clearDraft(threadId);
        this.autoScroll = true;
        // Fire-and-forget. On a hard send failure the service rolls back the
        // optimistic bubble; restore the draft so the user can retry (unless
        // they've already started typing a new one). The error banner set by
        // the service explains why. Re-persist explicitly — a programmatic
        // inputText assignment does not fire ngModelChange.
        void this.chat.sendMessage(text).then((ok) => {
            if (ok === false && !this.inputText.trim()) {
                this.inputText = text;
                saveDraft(threadId, text);
            }
        });

        // Resize textarea back
        setTimeout(() => {
            if (this.inputEl?.nativeElement) {
                this.inputEl.nativeElement.style.height = 'auto';
            }
        });
    }

    // ===== Composer attachment / camera / voice handlers =====

    /** Open the attachment menu. */
    openAttachmentMenu(): void {
        this.attachmentMenuOpen.set(true);
    }

    /** Close the attachment menu. */
    closeAttachmentMenu(): void {
        this.attachmentMenuOpen.set(false);
    }

    /** Open the OS file picker. */
    pickFile(): void {
        this.closeAttachmentMenu();
        this.fileInput?.nativeElement.click();
    }

    /** Capture a photo: hidden camera input on mobile, getUserMedia overlay on desktop. */
    pickCamera(): void {
        this.closeAttachmentMenu();
        if (this.isMobileDevice()) {
            this.cameraInput?.nativeElement.click();
        } else {
            void this.openDesktopCamera();
        }
    }

    /**
     * Queue accepted previews and surface a rejection (if any) in the
     * composer's error banner. Centralizes what all four attach entry points
     * (file picker, paste, camera, drop) do with a createFilePreviews()
     * result, including an ordering that matters: addAttachments() always
     * clears attachmentError (see persistent-chat.service.ts), so the
     * rejection must be set AFTER queuing previews, not before, or a
     * selection that both accepts some files and rejects others would have
     * its error wiped the instant it's set.
     */
    private applyFilePreviews({previews, rejected}: FilePreviewResult): void {
        this.chat.addAttachments(previews);
        const rejection = describeAttachmentRejection(rejected);
        if (rejection) {
            this.chat.attachmentError.set(this.transloco.translate(rejection.key, rejection.params));
        }
    }

    /** Handler for both `<input type=file>` (file picker and mobile camera). */
    async onFilesSelected(event: Event): Promise<void> {
        const input = event.target as HTMLInputElement;
        if (!input.files || input.files.length === 0) return;
        this.applyFilePreviews(await this.fileHandling.createFilePreviews(Array.from(input.files)));
        // Allow re-selecting the same file later.
        input.value = '';
    }

    /**
     * Paste-to-attach (#11): when the clipboard carries files — a screenshot,
     * a copied image, or a file from the OS file manager — divert them into
     * the same attachment flow as the Attach button and drag-drop, rather than
     * letting the browser paste a data URL (or nothing) into the textarea.
     * Plain-text pastes carry no file items, so they fall through untouched.
     */
    async onPaste(event: ClipboardEvent): Promise<void> {
        const files = extractClipboardFiles(event.clipboardData?.items, Date.now());
        if (files.length === 0) return; // text paste — let the default run
        event.preventDefault();
        this.applyFilePreviews(await this.fileHandling.createFilePreviews(files));
    }

    /** Drop one queued attachment. */
    removeAttachment(id: string): void {
        this.chat.removeAttachment(id);
    }

    /** Open the image preview dialog. */
    openImagePreview(preview: FilePreview): void {
        if (preview.type !== FileType.IMAGE || !preview.preview) return;
        this.imagePreviewName.set(preview.name);
        this.imagePreviewUrl.set(preview.preview);
    }

    closeImagePreview(): void {
        this.imagePreviewUrl.set(null);
        this.imagePreviewName.set('');
    }

    /** Begin a voice recording (tap to start; the strip's ✓/✕ ends it). */
    async startRecording(): Promise<void> {
        if (this.isRecording()) return;
        this.capStopTriggered = false;
        const config: RecordingConfig = {
            isHoldToRecord: true,
            maxDuration: this.maxRecordingSeconds,
            audioConstraints: {
                echoCancellation: true,
                noiseSuppression: true,
                autoGainControl: true,
            },
        };
        try {
            await this.voiceRecording.startRecording(config, this.waveformCanvas?.nativeElement);
        } catch (e: any) {
            // Surface a permission/hardware error in the same banner used by uploads.
            const msg = e?.name === 'NotAllowedError'
                ? this.transloco.translate('chat.composer.micDenied')
                : this.transloco.translate('chat.composer.micError');
            this.chat.attachmentError.set(msg);
        }
    }

    /**
     * Stop recording, transcribe the clip to editable composer text, and keep
     * the audio attached.
     *
     * The transcript is appended into `inputText` (never clobbering a typed
     * draft). Reactivity note: `inputText` is a plain field, so assigning it
     * does not schedule change detection on its own — the `addAttachments`
     * call below writes the `pendingAttachments` signal, which triggers the
     * pass that re-syncs the textarea to show the transcript. Keep
     * `addAttachments` as the last step.
     */
    async stopRecording(): Promise<void> {
        if (!this.isRecording()) return;
        const result = await this.voiceRecording.stopRecording();
        if (!result || result.duration < 1) return;
        const preview = await this.fileHandling.createAudioFilePreview(result);

        const threadId = this.chat.threadId();
        if (threadId) {
            this.isTranscribing.set(true);
            try {
                const res = await firstValueFrom(
                    this.api.transcribeVoice(threadId, preview.file),
                );
                if (res && res !== 'unavailable' && res.text.trim()) {
                    const transcript = res.text.trim();
                    this.inputText = this.inputText.trim()
                        ? `${this.inputText.trim()}\n\n${transcript}`
                        : transcript;
                    queueMicrotask(() => this.autoResizeInput());
                } else if (res === null) {
                    // Transport/server error — keep the audio, surface a notice.
                    this.chat.attachmentError.set(
                        this.transloco.translate('chat.composer.transcribeError'),
                    );
                }
                // 'unavailable' (no STT model configured) → attach audio silently.
            } finally {
                this.isTranscribing.set(false);
            }
        }

        this.chat.addAttachments([preview]);
    }

    cancelRecording(): void {
        this.voiceRecording.cancelRecording();
    }

    /** Desktop camera path: live MediaStream in a fullscreen overlay; capture to JPEG. */
    private async openDesktopCamera(): Promise<void> {
        let stream: MediaStream | null = null;
        try {
            stream = await navigator.mediaDevices.getUserMedia({video: true});
        } catch (e: any) {
            const msg = e?.name === 'NotAllowedError'
                ? this.transloco.translate('chat.composer.cameraDenied')
                : this.transloco.translate('chat.composer.cameraError');
            this.chat.attachmentError.set(msg);
            return;
        }

        // The overlay is appended to document.body so it sits above the
        // app shell and isn't constrained by any scoped scroll containers.
        // That also means scoped component styles don't reach it — inline
        // styles below.
        const overlay = document.createElement('div');
        overlay.style.cssText =
            'position:fixed;inset:0;background:rgba(0,0,0,0.92);' +
            'z-index:9999;display:flex;flex-direction:column;align-items:center;' +
            'justify-content:center;gap:20px;';
        const video = document.createElement('video');
        video.srcObject = stream;
        video.autoplay = true;
        video.playsInline = true;
        video.style.cssText = 'max-width:90vw;max-height:70vh;border-radius:var(--radius-control);background:#000;';
        const buttons = document.createElement('div');
        buttons.style.cssText = 'display:flex;gap:12px;';
        const captureBtn = document.createElement('button');
        captureBtn.type = 'button';
        captureBtn.textContent = this.transloco.translate('chat.composer.capturePhoto');
        captureBtn.style.cssText =
            'padding:10px 20px;font-size:14px;font-weight:500;border:none;' +
            'border-radius:var(--radius-control);background:#3399D6;color:#fff;cursor:pointer;';
        const cancelBtn = document.createElement('button');
        cancelBtn.type = 'button';
        cancelBtn.textContent = this.transloco.translate('common.cancel');
        cancelBtn.style.cssText =
            'padding:10px 20px;font-size:14px;font-weight:500;border:none;' +
            'border-radius:var(--radius-control);background:#444;color:#fff;cursor:pointer;';
        buttons.appendChild(captureBtn);
        buttons.appendChild(cancelBtn);
        overlay.appendChild(video);
        overlay.appendChild(buttons);
        document.body.appendChild(overlay);

        const cleanup = () => {
            stream?.getTracks().forEach((t) => t.stop());
            if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
        };

        await new Promise<void>((resolve) => (video.onloadedmetadata = () => resolve()));

        captureBtn.onclick = () => {
            const canvas = document.createElement('canvas');
            canvas.width = video.videoWidth;
            canvas.height = video.videoHeight;
            const ctx = canvas.getContext('2d');
            if (!ctx) {
                cleanup();
                return;
            }
            ctx.drawImage(video, 0, 0);
            canvas.toBlob(
                async (blob) => {
                    if (blob) {
                        const file = new File([blob], `photo-${Date.now()}.jpg`, {
                            type: 'image/jpeg',
                            lastModified: Date.now(),
                        });
                        this.applyFilePreviews(await this.fileHandling.createFilePreviews([file]));
                    }
                    cleanup();
                },
                'image/jpeg',
                0.9,
            );
        };
        cancelBtn.onclick = cleanup;
        overlay.onclick = (e) => {
            if (e.target === overlay) cleanup();
        };
    }

    // ===== Drag-and-drop file handling =====

    /** True when the dragged payload includes files (not text or HTML). */
    private hasFilePayload(event: DragEvent): boolean {
        const types = event.dataTransfer?.types;
        if (!types) return false;
        // Spec says types is DOMStringList; browsers expose Array-like.
        for (let i = 0; i < types.length; i++) {
            if (types[i] === 'Files') return true;
        }
        return false;
    }

    @HostListener('dragenter', ['$event'])
    onDragEnter(event: DragEvent): void {
        if (!this.hasFilePayload(event)) return;
        event.preventDefault();
        this.dragEnterCount++;
        if (this.dragEnterCount === 1) this.isDragOver.set(true);
    }

    @HostListener('dragover', ['$event'])
    onDragOver(event: DragEvent): void {
        if (!this.hasFilePayload(event)) return;
        // preventDefault on dragover is what tells the browser this is a
        // valid drop target. Without it, the drop event never fires.
        event.preventDefault();
        if (event.dataTransfer) event.dataTransfer.dropEffect = 'copy';
    }

    @HostListener('dragleave', ['$event'])
    onDragLeave(event: DragEvent): void {
        if (!this.hasFilePayload(event)) return;
        this.dragEnterCount = Math.max(0, this.dragEnterCount - 1);
        if (this.dragEnterCount === 0) this.isDragOver.set(false);
    }

    @HostListener('drop', ['$event'])
    async onDrop(event: DragEvent): Promise<void> {
        if (!this.hasFilePayload(event)) return;
        event.preventDefault();
        this.dragEnterCount = 0;
        this.isDragOver.set(false);

        const files = event.dataTransfer?.files;
        if (!files || files.length === 0) return;
        // Honour the same gating as the file picker — if the session is
        // disconnected, the upload would fail anyway; if its unit is parked,
        // the chip could never be sent.
        if (!this.chat.isConnected() || this.chat.isParked()) return;

        this.applyFilePreviews(await this.fileHandling.createFilePreviews(Array.from(files)));
    }

    onInputChange(value: string): void {
        this.composerDraftRevision++;
        const trimmed = value.trimStart();
        if (trimmed.startsWith('/')) {
            const query = trimmed.split(/\s/)[0].toLowerCase();
            this.slashQuery.set(query);
            this.showSlashMenu.set(this.filteredCommands().length > 0);
            this.slashSelectedIndex.set(0);
        } else {
            this.slashQuery.set('');
            this.showSlashMenu.set(false);
        }
        // Persist the draft synchronously so an abrupt reload (auth redirect)
        // can't lose it. Cleared on a successful send.
        saveDraft(this.chat.threadId(), value);
    }

    onMessagesScroll(): void {
        const el = this.messagesContainer?.nativeElement;
        if (!el) return;
        // Ignore the programmatic scroll we trigger while restoring position.
        if (this.isRestoringScroll) return;
        // Near the top → widen the window toward older history (scroll-preserving).
        if (el.scrollTop < 120 && this.chat.hasOlderTurns()) {
            this.loadOlderHistory();
        }
        // If user is within 80px of the bottom, re-enable auto-scroll; otherwise pause it.
        const nearBottom = isNearBottom(el.scrollTop, el.scrollHeight, el.clientHeight);
        this.autoScroll = nearBottom;
        this.scrolledAway.set(!nearBottom);
        if (nearBottom) {
            this.newMessageCount.set(0);
            this.chat.resetWindow(); // re-bound the DOM once back at the bottom
        }
    }

    jumpToLatest(): void {
        this.autoScroll = true;
        this.scrolledAway.set(false);
        this.newMessageCount.set(0);
        this.chat.resetWindow();
        this.lastSeenMessageCount = this.chat.turns().length;
        this.scrollToBottom();
    }

    /**
     * Widen the render window toward older history, preserving scroll position
     * so the viewport doesn't jump when older turns are prepended. The window
     * grows over the already-loaded conversation (no network call) — the
     * scroll-delta restore is salvaged from Advanced-LLM-Chat's scroll solution.
     */
    private loadOlderHistory(): void {
        const el = this.messagesContainer?.nativeElement;
        if (!el) return;
        const prevHeight = el.scrollHeight;
        const prevTop = el.scrollTop;
        this.isRestoringScroll = true;
        this.chat.loadOlderTurns();
        afterNextRender(
            () => {
                const cur = this.messagesContainer?.nativeElement;
                if (cur) {
                    cur.scrollTop = prevTop + (cur.scrollHeight - prevHeight);
                }
                // Release after the synthetic scroll event has fired and been ignored.
                setTimeout(() => (this.isRestoringScroll = false), 50);
            },
            {injector: this.injector},
        );
    }

    selectSlashCommand(cmd: SlashCommand): void {
        this.inputText = cmd.command + ' ';
        this.showSlashMenu.set(false);
        this.inputEl?.nativeElement?.focus();
    }

    pickSuggestion(s: DisplayedSuggestion): void {
        this.inputText = s.text;
        // inputText is a plain ngModel field, so this assignment fires no
        // ngModelChange and onInputChange never runs — the draft must be saved by
        // hand or a reload eats it. Same hazard denyOffer() documents below.
        saveDraft(this.chat.threadId(), this.inputText);
        setTimeout(() => {
            this.inputEl?.nativeElement?.focus();
            this.autoResizeInput();
        });
    }

    /** Accept the offer and resume the agent once the workspace lands. */
    upgradeAndContinue(tier: string): void {
        this.chat.upgradeWorkspace(tier === 'vm' ? 'vm' : 'sandbox', {thenContinue: true});
    }

    /** Accept the offer and leave the agent idle; the user drives from here. */
    upgradeOnly(tier: string): void {
        this.chat.upgradeWorkspace(tier === 'vm' ? 'vm' : 'sandbox');
    }

    /**
     * Decline the offer and hand the user a composer to say why.
     *
     * Deliberately does not send: a bare "denied" is worse than nothing, since
     * the agent's tool result already told it a human would decide, so it needs
     * the reason to choose what to do instead. Unlike pickSuggestion this must
     * not clobber a half-typed message, and because inputText is a plain ngModel
     * field the assignment fires no ngModelChange — so the draft is saved by
     * hand or a reload eats it.
     */
    denyOffer(): void {
        this.chat.dismissWorkspaceOffer();
        this.inputText = composeDenyPrefill(
            this.inputText,
            this.transloco.translate('chat.workspaceOffer.denyStarter'),
        );
        saveDraft(this.chat.threadId(), this.inputText);
        setTimeout(() => {
            this.inputEl?.nativeElement?.focus();
            this.autoResizeInput();
        });
    }

    /** The user turn the rewind sheet is open for (null = closed). */
    rewindTarget = signal<UserTurn | null>(null);

    openRewindSheet(turn: UserTurn): void {
        this.rewindTarget.set(turn);
        void this.chat.prepareRewind(turn.id);
    }

    closeRewindSheet(): void {
        this.rewindTarget.set(null);
        this.chat.rewindPreview.set(null);
    }

    /** /rewind target picker (newest prompt first). Same eligibility gate as
     *  the inline hover button: only historical turns that aren't still in
     *  the send outbox can anchor a rewind. */
    readonly rewindPickerOpen = signal(false);
    readonly rewindPickerQuery = signal('');
    readonly rewindFilterMin = REWIND_FILTER_MIN_CANDIDATES;

    readonly rewindCandidates = computed<UserTurn[]>(() =>
        pickRewindCandidates(this.chat.visibleTurns(), this.chat.outboxIds()));

    readonly filteredRewindCandidates = computed<UserTurn[]>(() =>
        filterRewindCandidates(this.rewindCandidates(), this.rewindPickerQuery()));

    openRewindPicker(): void {
        if (!this.chat.rewindModeAvailable('conversation')) return;
        this.rewindPickerQuery.set('');
        this.rewindPickerOpen.set(true);
        // The dialog's focus trap auto-captures onto its close button; move
        // initial focus where the interaction starts — the filter when it's
        // shown, the newest prompt otherwise. Delayed so it runs after the
        // trap's own deferred capture.
        setTimeout(() => {
            document
                .querySelector<HTMLElement>('.rewind-picker-filter, .rewind-picker-item')
                ?.focus();
        }, 50);
    }

    closeRewindPicker(): void {
        this.rewindPickerOpen.set(false);
    }

    pickRewindTarget(turn: UserTurn): void {
        this.rewindPickerOpen.set(false);
        this.openRewindSheet(turn);
    }

    /** Arrow-key navigation across picker rows; ArrowDown from the filter
     *  input drops into the list. Enter activates the focused row natively. */
    onRewindPickerKeydown(event: KeyboardEvent): void {
        const key = event.key;
        if (key !== 'ArrowDown' && key !== 'ArrowUp' && key !== 'Home' && key !== 'End') return;
        const items = Array.from(document.querySelectorAll<HTMLElement>('.rewind-picker-item'));
        if (items.length === 0) return;
        const active = document.activeElement as HTMLElement | null;
        // Leave Home/End alone inside the filter input — they mean caret jumps there.
        const inFilter = active?.classList.contains('rewind-picker-filter') ?? false;
        if (inFilter && (key === 'Home' || key === 'End')) return;
        event.preventDefault();
        const idx = active ? items.indexOf(active) : -1;
        let next: number;
        if (key === 'Home') next = 0;
        else if (key === 'End') next = items.length - 1;
        else if (idx === -1) next = key === 'ArrowDown' ? 0 : items.length - 1;
        else next = Math.min(items.length - 1, Math.max(0, idx + (key === 'ArrowDown' ? 1 : -1)));
        items[next]?.focus();
        items[next]?.scrollIntoView({block: 'nearest'});
    }

    rewindStamp(ts: number): string {
        return formatRewindStamp(
            ts,
            Date.now(),
            this.transloco.getActiveLang(),
            this.transloco.translate('chat.rewind.yesterday'),
        );
    }

    /** Meta line under the action-sheet quote: when the target was sent, and
     *  how many later messages a conversation rewind hides (omitted at 0 —
     *  "hides 0 messages" reads worse than saying nothing). */
    rewindTargetStamp(): string {
        const target = this.rewindTarget();
        return target ? this.rewindStamp(target.timestamp) : '';
    }

    rewindHiddenCount(): number {
        const target = this.rewindTarget();
        if (!target) return 0;
        return Math.max(0, countTurnsAfter(this.chat.visibleTurns(), target.id));
    }

    confirmRewind(mode: 'both' | 'conversation' | 'code'): void {
        const target = this.rewindTarget();
        if (!target) return;
        this.chat.rewind(target.id, mode, this.inputText, this.composerDraftRevision);
        this.rewindTarget.set(null);
    }

    insertRewindPrompt(prefill: RewindPrefill): void {
        this.inputText = prefill.prompt;
        this.composerDraftRevision++;
        saveDraft(this.chat.threadId(), this.inputText);
        this.chat.consumeRewindPrefill(prefill.clientRequestId);
        setTimeout(() => {
            this.inputEl?.nativeElement?.focus();
            this.autoResizeInput();
        });
    }

    reviewQueuedSend(localId: string): void {
        const text = this.chat.takeQueuedSendForReview(localId);
        if (text === null) return;
        this.inputText = text;
        this.composerDraftRevision++;
        saveDraft(this.chat.threadId(), text);
        setTimeout(() => {
            this.inputEl?.nativeElement?.focus();
            this.autoResizeInput();
        });
    }

    confirmSummarizeUpTo(): void {
        const target = this.rewindTarget();
        if (!target) return;
        this.chat.summarizeUpTo(target.id);
        this.rewindTarget.set(null);
    }

    /** First 160 chars of the target prompt for the dialog header quote
     *  (avoids importing SlicePipe into this standalone component). */
    rewindQuote(): string {
        const text = this.rewindTarget()?.content || '';
        return text.length > 160 ? text.slice(0, 160) + '…' : text;
    }

    onKeydown(event: KeyboardEvent): void {
        // Slash menu navigation
        if (this.showSlashMenu() && this.filteredCommands().length > 0) {
            const cmds = this.filteredCommands();
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                this.slashSelectedIndex.update(i => Math.min(i + 1, cmds.length - 1));
                return;
            }
            if (event.key === 'ArrowUp') {
                event.preventDefault();
                this.slashSelectedIndex.update(i => Math.max(i - 1, 0));
                return;
            }
            if (event.key === 'Tab' || (event.key === 'Enter' && !event.shiftKey)) {
                event.preventDefault();
                const selected = cmds[this.slashSelectedIndex()];
                if (selected) {
                    this.selectSlashCommand(selected);
                }
                return;
            }
            if (event.key === 'Escape') {
                this.showSlashMenu.set(false);
                return;
            }
        }

        if (event.key === 'Enter') {
            // Touch devices: let Enter fall through as a newline — the send
            // button is the send affordance there (see shouldSendOnEnter).
            if (!shouldSendOnEnter(event.shiftKey, this.isMobileDevice())) return;
            event.preventDefault();
            this.send();
        }
    }

    approveAndAutoAccept(): void {
        this.chat.setMode('auto_accept');
        this.chat.approveAll();
    }

    /** Display-only: whether reasoning ("thinking") blocks open expanded by default. */
    onReasoningDefaultChange(value: string | null): void {
        if (value === 'expanded' || value === 'collapsed') {
            this.chatPrefs.setReasoningExpanded(value === 'expanded');
        }
    }

    /** Display-only: whether tool-call runs render inline ("expanded") or folded. */
    onToolCallsDefaultChange(value: string | null): void {
        if (value === 'expanded' || value === 'collapsed') {
            this.chatPrefs.setToolCallsExpanded(value === 'expanded');
        }
    }

    /** Display-only: the reading-column width preset (Comfortable / Wide / Full). */
    onReadingWidthChange(value: string | null): void {
        if (value === 'comfortable' || value === 'wide' || value === 'full') {
            this.chatPrefs.setReadingWidth(value);
        }
    }

    /** Display-only: the prose text-size preset (Small / Medium / Large). */
    onTextSizeChange(value: string | null): void {
        if (value === 'small' || value === 'medium' || value === 'large') {
            this.chatPrefs.setTextSize(value);
        }
    }

    openIde(url: string): void {
        window.open(url, '_blank');
    }

    openCodeServer(): void {
        // Pre-flight the IDE status through the auth interceptor before opening:
        // the XHR validates/bumps the BFF session and, on a 401 (idle-expired
        // session), the interceptor redirects to re-login — so a direct
        // window.open never dumps raw 401 JSON. Mirrors job-list.component.ts.
        const threadId = this.chat.threadId();
        if (!threadId) return;
        this.api.getThreadIdeStatus(threadId).subscribe(status => {
            this.ideStatus.set(status);
            const url = pickCodeServerUrlToOpen(status);
            if (url) window.open(url, '_blank');
        });
    }

    /**
     * The session scratch folder is only relabelled when a project-folder
     * action sits beside it. On an ordinary session there is nothing to
     * disambiguate from, and "Files" stays "Files".
     */
    readonly sessionFilesLabelKey = computed(() =>
        this.chat.verifiedProjectFolder()
            ? 'chat.header.sessionFilesButton'
            : 'chat.header.filesButton',
    );

    /** PC-19: the protected project folder, never a guessed URL. Only ever
     *  reachable when `verifiedProjectFolder` resolved one. */
    openProjectFiles(): void {
        const url = this.chat.verifiedProjectFolder()?.url;
        if (url) window.open(url, '_blank');
    }

    openSessionFiles(): void {
        // Prefer the backend-computed URL (works for all backends).
        const cloudUrl = this.chat.cloudSessionUrl();
        if (cloudUrl) {
            window.open(cloudUrl, '_blank');
            return;
        }
        // Legacy fallback for Nextcloud sessions without a computed URL.
        const folder = this.chat.ncSessionFolder();
        if (!folder || !environment.cloudUrl) return;
        const folderName = folder.split('/').pop();
        window.open(`${environment.cloudUrl}/apps/files/?dir=/${folderName}`, '_blank');
    }

    async resumeSession(): Promise<void> {
        // Spinner state lives on the service (chat.isResuming) so the composer
        // gate and this button read one source — a send-triggered resume has no
        // click to hang a local flag off.
        try {
            await this.chat.resumeSession();
        } catch (e: any) {
            this.chat.error.set(e?.error?.detail || this.transloco.translate('chat.system.resumeFailed'));
        }
    }

    async disconnectAndLeave(): Promise<void> {
        if (this.isDisconnecting()) return;
        this.isDisconnecting.set(true);
        try {
            await this.chat.endSession();
        } catch (e: any) {
            this.toast.danger(this.errors.translate(e, 'errors.sessions.endFailed'));
        } finally {
            // Leaving destroys this component, so clearing the flag is normally
            // moot — but a guard can refuse the navigation, and then the header
            // has to come back rather than spin forever.
            const left = await this.router.navigate(['/sessions']);
            if (!left) this.isDisconnecting.set(false);
        }
    }

    async onRenameSession(threadId: string, title: string): Promise<void> {
        try {
            await this.chat.renameThread(threadId, title);
            // The rail has its own copy of the thread list (SessionListService)
            // and only refreshes it on NavigationEnd — a rename never navigates,
            // so without this the rail shows the old title until the user
            // leaves and comes back.
            this.sessionList.renameLocal(threadId, title);
        } catch (e) {
            this.toast.danger(this.errors.translate(e, 'errors.sessions.renameFailed'));
        }
    }

    /** ⋮ menu "Copy session ID …" row. Spec §6: the id is a debugging
     *  affordance, not wayfinding, so it lives behind the overflow menu
     *  instead of costing a permanent header chip — but it copies the full
     *  id, not the truncated one the menu label shows. */
    async copyThreadId(): Promise<void> {
        const threadId = this.chat.threadId();
        if (!threadId) return;
        if (await copyText(threadId)) {
            this.toast.success(this.transloco.translate('common.copied'));
        } else {
            this.toast.danger(this.transloco.translate('common.copyFailed'));
        }
    }

    private startIdePolling(threadId: string): void {
        this.stopIdePolling();
        this.idePollingAttempts = 0;
        // Fetch immediately, then poll every 10s
        this.fetchIdeStatus(threadId);
        this.idePollingTimer = setInterval(() => this.fetchIdeStatus(threadId), 10_000);
    }

    private stopIdePolling(): void {
        if (this.idePollingTimer) {
            clearInterval(this.idePollingTimer);
            this.idePollingTimer = null;
        }
    }

    private fetchIdeStatus(threadId: string): void {
        this.idePollingAttempts++;
        this.api.getThreadIdeStatus(threadId).subscribe(status => {
            this.ideStatus.set(status);
            // Stop polling once active or after 30 attempts (5 min)
            if (status?.status === 'active' || this.idePollingAttempts >= 30) {
                this.stopIdePolling();
            }
        });
    }

    /**
     * Park the viewport at the bottom, now, in this frame.
     *
     * `behavior: 'instant'` rather than a bare `scrollTop =` assignment: per
     * CSSOM-View the scrollTop setter scrolls with behavior 'auto', which
     * resolves to the *computed* `scroll-behavior` — so a single global
     * `html { scroll-behavior: smooth }` would silently animate every pin,
     * which would then chase a moving target during streaming and never settle.
     * The SCSS pins `scroll-behavior: auto` on .messages as the other half of
     * that guard.
     */
    private scrollToBottom(): void {
        const el = this.messagesContainer?.nativeElement;
        if (el) {
            el.scrollTo({top: pinTarget(el.scrollHeight, el.clientHeight), behavior: 'instant'});
        }
    }

    /** Wrap tall <pre> blocks in a collapsed <details> element. */
    private collapseCodeBlocks(): void {
        const container = this.messagesContainer?.nativeElement;
        if (!container) return;
        // Exclude <app-tool-card> internals (.tc__result / .tc__code): those are
        // structured UI with their own overflow + copy handling, not markdown
        // code blocks. Without this they'd be double-wrapped in a code-collapse.
        const blocks = container.querySelectorAll<HTMLPreElement>(
            '.message-body pre:not([data-collapsed]):not(.tc__result):not(.tc__code)',
        );
        for (const pre of Array.from(blocks)) {
            // Skip blocks still streaming — marking/wrapping them now destroys the
            // wrapper every flush (flicker) and reads scrollHeight on a growing
            // element. The class drops when the block completes; the next pass
            // processes the now-final <pre>.
            if (pre.closest('.streaming-block')) continue;
            pre.setAttribute('data-collapsed', '');
            if (pre.scrollHeight <= 200) continue;
            const lang = pre.querySelector('code')?.className?.match(/language-(\S+)/)?.[1] || '';
            const wrapper = document.createElement('details');
            wrapper.className = 'code-collapse';
            const summary = document.createElement('summary');
            summary.className = 'code-collapse-summary';
            const expandHint = this.transloco.translate('chat.code.expandHint');
            summary.innerHTML = `<span class="code-collapse-icon">code</span>`
                + `<span class="code-collapse-label">${lang || 'code'}</span>`
                + `<span class="code-collapse-hint">${expandHint}</span>`;
            wrapper.appendChild(summary);
            pre.parentNode!.insertBefore(wrapper, pre);
            wrapper.appendChild(pre);
        }
    }

    /** Add a copy-to-clipboard button to every <pre> block that doesn't have one yet. */
    private addCopyButtons(): void {
        const container = this.messagesContainer?.nativeElement;
        if (!container) return;
        // Tool-card pres carry their own copy button — skip them here (see
        // collapseCodeBlocks for the same exclusion rationale).
        const blocks = container.querySelectorAll<HTMLPreElement>(
            '.message-body pre:not([data-copy-btn]):not(.tc__result):not(.tc__code)',
        );
        for (const pre of Array.from(blocks)) {
            // Same streaming exclusion as collapseCodeBlocks — don't attach a
            // copy button inside a block that's still growing.
            if (pre.closest('.streaming-block')) continue;
            pre.setAttribute('data-copy-btn', '');
            const btn = document.createElement('button');
            btn.className = 'code-copy-btn';
            btn.innerHTML = '<span class="code-copy-icon">content_copy</span>';
            btn.title = this.transloco.translate('chat.code.copyTooltip');
            btn.addEventListener('click', () => {
                const code = pre.querySelector('code')?.textContent || pre.textContent || '';
                navigator.clipboard.writeText(code).then(() => {
                    btn.innerHTML = '<span class="code-copy-icon">check</span>';
                    setTimeout(() => {
                        btn.innerHTML = '<span class="code-copy-icon">content_copy</span>';
                    }, 1500);
                });
            });
            pre.style.position = 'relative';
            pre.appendChild(btn);
        }
    }

    // ===== Turn-bubble helpers =====

    /**
     * Auto-collapse threshold: assistant turns with more than this many events
     * fold their lead-up by default once they're done (the final answer stays
     * visible). Streaming turns are never auto-collapsed. The user can override
     * either way via the chevron, in which case userTurnCollapsed wins.
     */
    private readonly AUTO_COLLAPSE_THRESHOLD = 8;

    /**
     * Per-turn user override for collapse state. Keyed by turn id. Value is
     * `true` when the user explicitly collapsed it, `false` when they explicitly
     * expanded it. Absent = use the auto rule.
     */
    private readonly userTurnCollapsed = signal<Record<string, boolean>>({});

    /** Whether the given assistant turn should render in collapsed mode. */
    isTurnCollapsed(turn: AssistantTurn): boolean {
        const explicit = this.userTurnCollapsed()[turn.id];
        if (explicit !== undefined) return explicit;
        if (turn.status === 'streaming') return false;
        return turn.events.length > this.AUTO_COLLAPSE_THRESHOLD;
    }

    /** Toggle the user-explicit collapse state for a turn. */
    toggleTurnCollapse(turn: AssistantTurn): void {
        const wasCollapsed = this.isTurnCollapsed(turn);
        this.userTurnCollapsed.update((cur) => ({...cur, [turn.id]: !wasCollapsed}));
    }

    /** Per-type event counts for the chevron badge. */
    turnEventCounts(turn: AssistantTurn) {
        return countEvents(turn);
    }

    /** Officer lens (§3.2): fold quiet wake cycles on officer threads unless the user asked for everything. */
    readonly turnViews = computed<TurnView[]>(() => {
        const turns = this.chat.visibleTurns();
        if (this.chat.isOfficerThread() && this.chatPrefs.officerLensFolded()) {
            return foldWakeCycles(turns);
        }
        return turns.map((turn) => ({kind: 'turn' as const, id: turn.id, turn}));
    });

    onOfficerLensChange(value: string | null): void {
        this.chatPrefs.setOfficerLensFolded(value === 'folded');
    }

    /** Exposed for the wake-cycle template: what the officer said, if anything, before sleeping. */
    readonly trailingText = trailingText;

    /** Coalesce a turn's events into render groups (live edge pinned, rest folded). */
    // Memoized per turn object. The reducer rebuilds the turn immutably on
    // every update, so a changed turn is a new key and the cache invalidates
    // naturally — this turns 50 rebuilds per change-detection pass into 1.
    private readonly groupedEventsCache = new WeakMap<AssistantTurn, EventGroup[]>();

    groupedEvents(turn: AssistantTurn): EventGroup[] {
        const cached = this.groupedEventsCache.get(turn);
        if (cached) return cached;
        const groups = groupEvents(turn.events);
        this.groupedEventsCache.set(turn, groups);
        return groups;
    }

    /**
     * Officer→user messages (`notify_user`) shown in a COLLAPSED turn — the
     * one event type besides the final answer that collapsing never hides.
     * Memoized per turn object like {@link groupedEvents}.
     */
    private readonly notifyCallsCache = new WeakMap<AssistantTurn, ToolCallEvent[]>();

    collapsedNotifyCalls(turn: AssistantTurn): ToolCallEvent[] {
        const cached = this.notifyCallsCache.get(turn);
        if (cached) return cached;
        const calls = notifyToolCalls(turn);
        this.notifyCallsCache.set(turn, calls);
        return calls;
    }

    /**
     * Render-ready view-model for a tool call, memoized per event object so the
     * shared <app-tool-card> keeps a stable input identity (OnPush) across
     * change-detection cycles. The reducer recreates the event object on every
     * update, so the WeakMap key naturally invalidates when the call changes.
     */
    /**
     * Job whose diff drawer is open, or null. Separate from
     * `chat.cloudDiffPanelOpen` so a job's diff and the session's own staged
     * cloud changes can never be mistaken for one another.
     */
    readonly jobDiffId = signal<string | null>(null);

    openJobDiff(jobId: string): void {
        this.chat.cloudDiffPanelOpen.set(false);
        this.jobDiffId.set(jobId);
    }

    private readonly toolViewCache = new WeakMap<ToolCallEvent, ToolCardView>();

    toolView(tc: ToolCallEvent): ToolCardView {
        const cached = this.toolViewCache.get(tc);
        if (cached) return cached;
        const view = toolCardViewFromEvent(tc);
        this.toolViewCache.set(tc, view);
        return view;
    }

    /**
     * Views for a job fan-out, memoized per group object.
     *
     * Memoized for the same reason as {@link toolView}: `<app-job-batch-card>`
     * is OnPush with an array input, so building a fresh array each change
     * detection would defeat it. The key is safe because `groupedEvents()` is
     * itself memoized per turn — a group object is stable until the turn changes,
     * and a changed turn produces new groups.
     */
    private readonly jobBatchViewsCache = new WeakMap<object, ToolCardView[]>();

    jobBatchViews(group: EventGroup & {kind: 'job_batch'}): ToolCardView[] {
        const cached = this.jobBatchViewsCache.get(group);
        if (cached) return cached;
        const views = group.events.map((e) => this.toolView(e));
        this.jobBatchViewsCache.set(group, views);
        return views;
    }

    /**
     * Whether a folded run renders as a chip. The "Tool calls → Expanded"
     * preference is the escape hatch: with it on, nothing folds and every event
     * renders inline, exactly as before.
     */
    foldRun(events: FoldableEvent[]): boolean {
        return shouldFoldToolRun(events.length, this.chatPrefs.toolCallsExpanded(), MIN_FOLD_RUN);
    }

    /** Tool calls inside a folded run, for the expanded chip body. */
    foldedTools(events: FoldableEvent[]): ToolCallEvent[] {
        return events.filter((e): e is ToolCallEvent => e.kind === 'tool_call');
    }

    /** Thoughts inside a folded run, for the expanded chip body. */
    foldedThoughts(events: FoldableEvent[]): ThoughtEvent[] {
        return events.filter((e): e is ThoughtEvent => e.kind === 'thought');
    }

    private readonly foldedSummaryCache = new WeakMap<FoldableEvent[], FoldedSummary>();

    private summaryOf(events: FoldableEvent[]): FoldedSummary {
        const cached = this.foldedSummaryCache.get(events);
        if (cached) return cached;
        const s = summarizeFolded(events);
        this.foldedSummaryCache.set(events, s);
        return s;
    }

    /**
     * The chip's count line: "24× searches · 20× citations · 6× thoughts".
     * Categories, not types — a grand total plus per-category counts would
     * double-count, since commands *are* tool calls. Capped so the line stays
     * one row; the overflow is stated rather than silently dropped, and opening
     * the chip shows everything regardless.
     */
    foldedSummaryText(events: FoldableEvent[]): string {
        this.i18n.activeLang();
        const {parts} = this.summaryOf(events);
        const shown = parts.slice(0, CHIP_CATEGORY_CAP);
        const line = shown
            .map(({category, count}) => `${count}× ${this.categoryNoun(category, count)}`)
            .join(' · ');
        const hidden = parts.length - shown.length;
        return hidden > 0
            ? `${line} · ${this.transloco.translate('chat.turn.foldMore', {count: hidden})}`
            : line;
    }

    /** Failed/denied count in a folded run — badged on the chip. */
    foldedFailedCount(events: FoldableEvent[]): number {
        return this.summaryOf(events).failed;
    }

    /** Terse noun for a tool category, singular at count 1. i18n first, const map as fallback. */
    private categoryNoun(category: string, count: number): string {
        const one = count === 1;
        const key = `chat.${one ? 'categoryNounsOne' : 'categoryNouns'}.${category}`;
        const translated = this.transloco.translate(key);
        if (translated !== key) return translated;
        const map = one ? CATEGORY_NOUNS_ONE : CATEGORY_NOUNS;
        return map[category] ?? map['other'];
    }

    /** Last text event in a turn — used for the TTS "read aloud" button. */
    lastTextEvent(turn: AssistantTurn): TextEvent | undefined {
        return lastTextOf(turn);
    }

    /**
     * The turn's final answer — the trailing prose, recovered even when a
     * closing tool pass (e.g. the model registering citations after writing its
     * reply) trails it. Stays fully visible when the turn is collapsed (only the
     * lead-up folds). Empty only when the turn ends mid-work or has no text, in
     * which case the collapsed view falls back to {@link collapsedHeadline}.
     */
    finalAnswer(turn: AssistantTurn): string {
        return collapsedAnswer(turn);
    }

    /**
     * One-line fallback headline for a collapsed turn that has no closing prose
     * (ends on a tool/thought). Prefers the first sentence of the agent's
     * opening text; otherwise a tool/thought digest.
     */
    collapsedHeadline(turn: AssistantTurn): string {
        const first = firstTextOf(turn);
        const sentence = first ? firstSentence(first.content) : '';
        if (sentence) return sentence;
        const c = countEvents(turn);
        if (c.tools > 0) return this.transloco.translate('chat.turn.toolCount', {count: c.tools});
        if (c.thoughts > 0) return this.transloco.translate('chat.turn.thoughtCount', {count: c.thoughts});
        return this.transloco.translate('chat.turn.collapsedEmpty');
    }

    /**
     * Threshold for autocollapsing user messages. Content with more than this
     * many lines folds into a <details> summary so a multi-screen prompt
     * doesn't dominate the scroll.
     */
    private readonly USER_MESSAGE_COLLAPSE_LINES = 8;

    /** True when a user message exceeds the autocollapse line threshold. */
    isUserMessageLong(content: string): boolean {
        if (!content) return false;
        let lines = 1;
        for (let i = 0; i < content.length; i++) {
            if (content.charCodeAt(i) === 10) {
                lines++;
                if (lines > this.USER_MESSAGE_COLLAPSE_LINES) return true;
            }
        }
        return false;
    }

    /** First line of a user message — shown in the collapsed summary. */
    userMessagePreview(content: string): string {
        if (!content) return '';
        const newlineIdx = content.indexOf('\n');
        return newlineIdx === -1 ? content : content.slice(0, newlineIdx);
    }

    /**
     * The queued bubble's upload stage line: which i18n key to show and its
     * interpolation params, or null once there's nothing left to report.
     * Thin glue over `uploadStageFor` — gathers whether `localId` is the
     * outbox head, its own upload summary (used only when it is the head),
     * and the head's first not-yet-done file (used only when it isn't), then
     * lets the pure function decide. See `uploadStageFor` for why a non-head
     * item's own files are never shown.
     */
    uploadStage(localId: string): UploadStageView | null {
        const head = this.chat.outbox()[0];
        const isHead = head?.localId === localId;
        const ownFiles = this.chat.outboxItem(localId)?.pendingFiles;
        const ownSummary = ownFiles ? uploadSummary(ownFiles) : null;
        const headBlockingFileName = head?.pendingFiles?.find((f) => f.status !== 'done')?.name ?? null;
        const line = uploadStageFor(isHead, ownSummary, headBlockingFileName);
        if (!line) return null;
        return {
            ...line,
            announceKey: uploadStageAnnounceKey(line.key),
            // ONE indicator per message, and only on the item whose bytes are
            // actually moving: a queued non-head item is waiting on the head's
            // upload, so giving it a bar of its own would draw two bars for one
            // set of bytes.
            bar: isHead && !!ownSummary && ownSummary.total > 0,
            percent: isHead ? (ownSummary?.percent ?? null) : null,
        };
    }

    /** Divider placement under the lens: boundaries are judged between VIEWS (a folded cycle counts as its wake). */
    showSessionDividerAfterView(view: TurnView, index: number): boolean {
        return isSessionBoundary(lastTurnOf(view), nextSpeakingTurn(this.turnViews(), index));
    }

    // Tool call display helpers

    toolLabel(tc: ToolCallInfo): string {
        // Reference activeLang so Angular re-renders when the language changes.
        this.i18n.activeLang();
        const translatedKey = `chat.toolLabels.${tc.tool}`;
        const translated = this.transloco.translate(translatedKey);
        const base = translated !== translatedKey ? translated : TOOL_LABELS[tc.tool];
        if (base) {
            const context = this.toolLabelContext(tc.tool, tc.args);
            return context ? `${base} ${context}` : base;
        }
        return this.fallbackToolLabel(tc.tool);
    }

    /** Full argument content so the user can see WHAT each call does before
     *  approving the batch — see formatPermissionArgs (must never truncate:
     *  that's the whole safety model for a destructive call in the batch). */
    /** Template bridges to the pure key-pickers (see their definitions). */
    permissionTitleKey = permissionTitleKey;
    permissionApproveKey = permissionApproveKey;

    permissionArgs(perm: PermissionRequest): string {
        return formatPermissionArgs(perm.args);
    }

    formatTime(d: Date | string | number): string {
        const date = d instanceof Date ? d : new Date(d);
        return `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
    }

    private toolLabelContext(tool: string, args: Record<string, unknown>): string {
        if (!args) return '';
        const t = tool.toLowerCase();

        // File operations: show the filename
        if (t.includes('read') || t.includes('write') || t.includes('edit') || t === 'list_files' || t === 'file_exists') {
            const filePath = (args['file_path'] || args['path'] || args['file'] || args['pattern'] || '') as string;
            if (filePath) {
                const name = filePath.split('/').pop() || filePath;
                return name.length > 40 ? name.substring(0, 40) + '...' : name;
            }
        }

        // Grep/search: show the pattern
        if (t.includes('grep') || t.includes('search')) {
            const pattern = (args['pattern'] || args['query'] || '') as string;
            if (pattern) {
                const display = pattern.length > 30 ? pattern.substring(0, 30) + '...' : pattern;
                return this.transloco.translate('chat.toolContext.searchFor', {term: display});
            }
        }

        // Shell: show truncated command
        if (t === 'run_command' || t === 'shell_execute' || t === 'bash') {
            const cmd = (args['command'] || args['description'] || '') as string;
            if (cmd) {
                const display = cmd.length > 40 ? cmd.substring(0, 40) + '...' : cmd;
                return this.transloco.translate('chat.toolContext.commandDash', {cmd: display});
            }
        }

        return '';
    }

    private fallbackToolLabel(name: string): string {
        return name
            .replace(/_/g, ' ')
            .replace(/([a-z])([A-Z])/g, '$1 $2')
            .replace(/^./, c => c.toUpperCase());
    }

    groupToolCallsHuman(calls: ToolCallInfo[]): string {
        const groups = new Map<string, { count: number; label: string }>();
        for (const tc of calls) {
            const existing = groups.get(tc.tool);
            if (existing) {
                existing.count++;
            } else {
                groups.set(tc.tool, {count: 1, label: this.toolLabel(tc)});
            }
        }
        return Array.from(groups.values())
            .map(({count, label}) => count > 1 ? `${label} x${count}` : label)
            .join(', ');
    }

    currentToolLabelHuman(calls: ToolCallInfo[]): string {
        const running = calls.filter(tc => tc.status === 'running' || tc.status === 'pending');
        if (running.length === 0) return '';
        if (running.length === 1) return this.toolLabel(running[0]);
        const unique = new Map<string, ToolCallInfo>();
        for (const tc of running) {
            if (!unique.has(tc.tool)) unique.set(tc.tool, tc);
        }
        if (unique.size === 1) {
            const first = unique.values().next().value!;
            return `${this.toolLabel(first)} (${running.length})`;
        }
        return Array.from(unique.values()).map(tc => this.toolLabel(tc)).join(', ');
    }

    hasDeniedTools(calls: ToolCallInfo[]): boolean {
        return calls.some(tc => tc.status === 'denied');
    }

    groupToolCallsByIntent(calls: ToolCallInfo[]): string {
        this.i18n.activeLang();
        const groups = new Map<string, number>();
        for (const tc of calls) {
            const cat = tc.category || '';
            const catKey = cat ? `chat.toolCategories.${cat}` : '';
            const translated = catKey ? this.transloco.translate(catKey) : '';
            const fromI18n = translated && translated !== catKey ? translated : '';
            const label = fromI18n || CATEGORY_LABELS[cat] || this.toolLabel(tc);
            groups.set(label, (groups.get(label) || 0) + 1);
        }
        return Array.from(groups.entries())
            .map(([label, count]) => count > 1 ? `${label} x${count}` : label)
            .join(', ');
    }

    toolSummaryLabel(calls: ToolCallInfo[]): string {
        // Use intent grouping when categories are available, else human labels
        const hasCategories = calls.some(tc => tc.category);
        return hasCategories ? this.groupToolCallsByIntent(calls) : this.groupToolCallsHuman(calls);
    }

    groupToolCalls(calls: ToolCallInfo[]): string {
        const counts = new Map<string, number>();
        for (const tc of calls) {
            counts.set(tc.tool, (counts.get(tc.tool) || 0) + 1);
        }
        return Array.from(counts.entries())
            .map(([name, count]) => count > 1 ? `${name} x${count}` : name)
            .join(', ');
    }

    toolSummaryStatus(calls: ToolCallInfo[]): string {
        if (calls.some(tc => tc.status === 'error')) return 'error';
        if (calls.some(tc => tc.status === 'denied')) return 'denied';
        if (calls.some(tc => tc.status === 'running')) return 'running';
        if (calls.every(tc => tc.status === 'completed')) return 'completed';
        return 'mixed';
    }

    formatToolArgs(args: Record<string, unknown>): string {
        if (!args) return '';
        const entries = Object.entries(args);
        if (entries.length === 0) return '';
        for (const [, v] of entries) {
            if (typeof v === 'string' && v.length > 0) {
                return v.length > 60 ? v.substring(0, 60) + '...' : v;
            }
        }
        return JSON.stringify(args).substring(0, 60) + '...';
    }

    hasRunningTools(calls: ToolCallInfo[]): boolean {
        return calls.some(tc => tc.status === 'running' || tc.status === 'pending');
    }

    hasCompletedTools(calls: ToolCallInfo[]): boolean {
        return calls.some(tc => tc.status === 'completed' || tc.status === 'denied' || tc.status === 'error' || tc.status === 'expired');
    }

    completedOnly(calls: ToolCallInfo[]): ToolCallInfo[] {
        return calls.filter(tc => tc.status === 'completed' || tc.status === 'denied' || tc.status === 'error' || tc.status === 'expired');
    }

    completedToolCount(calls: ToolCallInfo[]): number {
        return calls.filter(tc => tc.status === 'completed' || tc.status === 'denied' || tc.status === 'error' || tc.status === 'expired').length;
    }

    currentToolLabel(calls: ToolCallInfo[]): string {
        const running = calls.filter(tc => tc.status === 'running' || tc.status === 'pending');
        if (running.length === 0) return '';
        if (running.length === 1) return running[0].tool;
        const unique = [...new Set(running.map(tc => tc.tool))];
        return unique.length === 1 ? `${unique[0]} (${running.length})` : unique.join(', ');
    }

    toolIcon(name: string): string {
        const t = name.toLowerCase();
        if (t.includes('read') || t.includes('write') || t.includes('edit') || t.includes('list_files')) return 'description';
        if (t.includes('git')) return 'history';
        if (t.includes('bash') || t.includes('shell') || t.includes('run_command')) return 'terminal';
        if (t.includes('web') || t.includes('search') || t.includes('fetch')) return 'public';
        if (t.includes('grep') || t.includes('search_files')) return 'search';
        return 'build';
    }

    previewResult(result: string): string {
        const line = result.trim().split('\n')[0];
        return line.length > 120 ? line.substring(0, 120) + '...' : line;
    }
}
