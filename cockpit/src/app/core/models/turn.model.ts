import {ChatAttachment, ToolCallInfo} from '../services/persistent-chat.service';
import {DELEGATE_TOOL, JOB_TOOL, NOTIFY_USER_TOOL, SLEEP_TOOL} from './tool-card.model';

/**
 * Turn-based conversation model for the persistent chat UI.
 *
 * A "turn" is one collapsible chat bubble — for an assistant, that's the
 * span between a user prompt and the next `end_turn` (final text with no
 * further tool calls). A turn carries an ordered list of typed events
 * (thoughts, plain-text blocks, and tool calls) as the agent emits them.
 *
 * See `knowledge-base/knowledge/features/session_turn_rendering.md` for the design rationale.
 * The model is intentionally flat-with-types rather than nested: thoughts
 * and tools have a 1:N (not 1:1) cardinality at the event level, and the
 * renderer is free to merge them visually without distorting the data.
 */

export type ThoughtStatus = 'streaming' | 'done' | 'hidden';
export type TextStatus = 'streaming' | 'done';

export interface ThoughtEvent {
    kind: 'thought';
    /** Stable id: `${turnId}.b${blockIndex}`. Survives SSE replay. */
    id: string;
    /**
     * Id of the AI message this reasoning belongs to (the thread_messages row
     * id; matches the live `thinking` frame's `message_id`). Set for reasoning
     * delivered via `reasoning_content` (gemma/DeepSeek/OpenRouter). Used to
     * dedupe a frame replayed after history already rendered the bubble. May be
     * undefined for older rows or interleaved Anthropic/Responses thinking.
     */
    messageId?: string;
    /** Accumulated content. Empty when status === 'hidden'. */
    content: string;
    status: ThoughtStatus;
    startedAt: number;
    durationMs?: number;
}

export interface TextEvent {
    kind: 'text';
    id: string;
    content: string;
    status: TextStatus;
    startedAt: number;
}

/**
 * Tool-call event. Extends ToolCallInfo so the existing component helpers
 * (toolLabel, groupToolCallsHuman, etc.) keep working unchanged.
 */
export interface ToolCallEvent extends ToolCallInfo {
    kind: 'tool_call';
    startedAt: number;
    durationMs?: number;
    /** Optional secondary status for nicer rendering (exit code, error class). */
    resultStatus?: 'ok' | 'error' | 'denied';
    exitCode?: number;
    /**
     * Which assistant message carried this call. Calls with the same key were
     * issued together, in one model response. History sets the AI row id; the
     * live stream carries no message id on tool frames, so the reducer stamps
     * a synthetic key there (see `liveMessageKey` in `turn-reducer.ts`).
     * Compare keys for equality only — the two sources never mix within one
     * call. Absent on events built before this existed; grouping then falls
     * back to contiguity.
     */
    messageKey?: string;
}

/**
 * Mid-turn context-compaction marker. A `role='summary'` row whose
 * `turn_number` falls inside a grouped assistant turn renders as an inline
 * event at its true position in the event stream — the turn block anchors at
 * its first row, so a top-level CompactionTurn divider would otherwise trail
 * the whole turn's content (the "summary rendered below the reply" bug).
 */
export interface CompactionEvent {
    kind: 'compaction';
    id: string;
    /** The summary the agent produced. May be empty when unavailable. */
    summary: string;
    startedAt: number;
}

export type TurnEvent = ThoughtEvent | TextEvent | ToolCallEvent | CompactionEvent;

export type AssistantTurnStatus = 'streaming' | 'done' | 'interrupted' | 'error';

export interface AssistantTurn {
    kind: 'assistant';
    id: string;
    events: TurnEvent[];
    status: AssistantTurnStatus;
    /**
     * Logical persistent-loop turn number. REST history uses message UUIDs as
     * bubble ids, while live SSE uses this number as `turn_id`; retaining both
     * lets a cold reattach join an incrementally persisted prefix back to the
     * still-running live turn.
     */
    turnNumber?: number;
    /** Model identifier, populated from the backend `turn.started.model` when present. */
    model?: string;
    startedAt: number;
    finishedAt?: number;
    /** Optional totals — populated in Phase 3, ignored in Phase 1. */
    totals?: { inputTokens?: number; outputTokens?: number; costUsd?: number };
    /** True for turns rehydrated from REST history (not streamed live this session). */
    historical?: boolean;
    /** True for turns synthesised by the reducer to absorb streaming events
     * that arrived without a preceding `turn.started` (e.g. SSE replay
     * cursor past the start event after a mid-turn reconnect). See
     * knowledge-base/knowledge/issues/persistent_chat_lost_assistant_turn_on_mid_turn_reload.md
     * §Approach 2. The turn gets promoted to the real id (or closed) when
     * `turn.completed` / `turn.interrupted` finally arrives. */
    recovered?: boolean;
}

export interface UserTurn {
    kind: 'user';
    id: string;
    content: string;
    attachments?: ChatAttachment[];
    timestamp: number;
    historical?: boolean;
}

/**
 * System-emitted line — slash-command echoes, session lifecycle markers,
 * VM-upgrade notices. Renders as a single muted line, not a bubble.
 */
export interface SystemTurn {
    kind: 'system';
    id: string;
    content: string;
    timestamp: number;
}

/**
 * Compaction boundary — emitted when the agent summarized the conversation
 * (manual `/compact` or automatic `ensure_within_limits`). Renders as a
 * centered divider banner (like the session-ended marker), expandable to the
 * summary text, so the user can see the exact state the agent works from.
 */
export interface CompactionTurn {
    kind: 'compaction';
    id: string;
    /** The summary the agent produced. May be empty when unavailable. */
    summary: string;
    timestamp: number;
}

export type Turn = AssistantTurn | UserTurn | SystemTurn | CompactionTurn;

/**
 * Top-level conversation state held in PersistentChatService.
 * One object replaces the old `messages`, `streamingText`, `streamingThinking`,
 * and `currentToolCalls` signals.
 */
export interface ConversationState {
    threadId: string | null;
    turns: Turn[];
    /** Id of the in-flight assistant turn, or null when no turn is streaming. */
    activeAssistantTurnId: string | null;
}

export const EMPTY_CONVERSATION: ConversationState = {
    threadId: null,
    turns: [],
    activeAssistantTurnId: null,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export function isAssistantTurn(t: Turn): t is AssistantTurn {
    return t.kind === 'assistant';
}

export function isUserTurn(t: Turn): t is UserTurn {
    return t.kind === 'user';
}

export function isSystemTurn(t: Turn): t is SystemTurn {
    return t.kind === 'system';
}

export function isCompactionTurn(t: Turn): t is CompactionTurn {
    return t.kind === 'compaction';
}

export function isThought(e: TurnEvent): e is ThoughtEvent {
    return e.kind === 'thought';
}

export function isText(e: TurnEvent): e is TextEvent {
    return e.kind === 'text';
}

export function isToolCall(e: TurnEvent): e is ToolCallEvent {
    return e.kind === 'tool_call';
}

/**
 * The officer→user messages (`notify_user` calls) in a turn. These render as
 * first-class chat bubbles and must stay visible even when the turn is
 * collapsed — collapsing folds the *lead-up*, and a message addressed to the
 * user is never lead-up.
 */
export function notifyToolCalls(turn: AssistantTurn): ToolCallEvent[] {
    return turn.events.filter(
        (e): e is ToolCallEvent => e.kind === 'tool_call' && e.tool === NOTIFY_USER_TOOL,
    );
}

/**
 * Returns the last text event in a turn (the "headline" used in the
 * collapsed-turn view). Returns undefined for turns without any text.
 */
export function lastTextOf(turn: AssistantTurn): TextEvent | undefined {
    for (let i = turn.events.length - 1; i >= 0; i--) {
        const e = turn.events[i];
        if (isText(e)) return e;
    }
    return undefined;
}

/**
 * Returns the first text event in a turn. The agent's opening line usually
 * states what it's about to do, which makes a far more useful collapsed
 * headline than the trailing text (often just "Done."). Symmetric with
 * lastTextOf.
 */
export function firstTextOf(turn: AssistantTurn): TextEvent | undefined {
    for (const e of turn.events) {
        if (isText(e)) return e;
    }
    return undefined;
}

/**
 * The turn's "final answer": the trailing run of text events — the prose the
 * model ended the turn on. This is the part worth keeping fully visible even
 * when the turn is collapsed; collapsing folds only the lead-up (opening text,
 * reasoning, tool calls), never the answer. Multiple trailing text blocks are
 * joined with a blank line.
 *
 * Stray non-answer events AFTER the closing prose are tolerated: a finished
 * thought (transports that only hand reasoning over post-stream broadcast it
 * after the answer tokens — reasoning always precedes the answer at the model
 * level) or an inline compaction marker must not blank the collapsed answer.
 * A still-streaming thought means the model is mid-work, so the text above it
 * is not the final answer yet; a thought BETWEEN texts likewise cuts the run
 * (the earlier text is lead-up, not answer). Tool calls always terminate.
 *
 * Returns '' when the turn ends on a tool call (no closing prose), is still
 * thinking, or has no text — callers fall back to the one-line headline.
 */
export function trailingText(turn: AssistantTurn): string {
    const parts: string[] = [];
    for (let i = turn.events.length - 1; i >= 0; i--) {
        const e = turn.events[i];
        if (isText(e)) {
            parts.push(e.content);
            continue;
        }
        const skippable =
            (isThought(e) && e.status !== 'streaming') || e.kind === 'compaction';
        if (parts.length === 0 && skippable) continue;
        break;
    }
    return parts.reverse().join('\n\n').trim();
}

/**
 * The prose to show for a COLLAPSED turn.
 *
 * Prefers {@link trailingText} — the answer the model closed the turn on. When
 * that's empty because the turn ended on *tool calls* rather than prose, recover
 * the answer anyway: skip the trailing completed tool calls (and finished
 * thoughts / compaction markers) and return the last contiguous run of text
 * blocks behind them.
 *
 * This is the "answer, then a closing tool pass" case. Its most common shape is
 * a citation pass — the model writes its reply with inline [N] markers, then
 * calls cite_web/cite_document to register each one (see
 * knowledge-base/knowledge/features/session_turn_rendering.md) — but a trailing verification
 * `run_command` or a final `save_file` produces the same shape, so this is
 * category-agnostic rather than citation-specific (history rows may not even
 * carry a stamped category). Without it, a collapsed turn ending on such a pass
 * would drop to the one-line opening headline and hide the actual answer.
 *
 * In-flight work still yields '' (→ headline): a running tool or a streaming
 * thought at the tail means the turn isn't finished, so the text above it is
 * lead-up, not a final answer, and must not be surfaced as one. Returns '' when
 * the turn has no text at all.
 */
export function collapsedAnswer(turn: AssistantTurn): string {
    const closing = trailingText(turn);
    if (closing) return closing;
    const parts: string[] = [];
    for (let i = turn.events.length - 1; i >= 0; i--) {
        const e = turn.events[i];
        if (isText(e)) {
            parts.push(e.content);
            continue;
        }
        if (parts.length > 0) break; // reached the lead-up before the answer run
        if (isEventInFlight(e)) break; // still working → no final answer yet
        // else: a completed tool call / finished thought / compaction marker
        // trailing the answer — skip it and keep looking back for the prose.
    }
    return parts.reverse().join('\n\n').trim();
}

/**
 * First sentence of a (possibly markdown) block, for a one-line headline.
 * Drops leading markdown markers, collapses whitespace, cuts at the first
 * sentence terminator at/after a sensible minimum length, and caps the
 * result. Returns '' for empty/whitespace input.
 */
export function firstSentence(text: string, maxLen = 140): string {
    if (!text) return '';
    // Strip a leading/trailing code fence (```lang … ```) so a turn whose first
    // text is a fenced code block reads as the code's first line rather than
    // literal backticks. Then drop a leading markdown marker (#, >, bullet) and
    // collapse whitespace so the headline reads as prose, not raw source.
    let s = text
        .trim()
        .replace(/^`{3,}[^\n]*\r?\n?/, '')
        .replace(/\r?\n?`{3,}\s*$/, '')
        .replace(/^[#>\s]*[-*]?\s*/, '')
        .replace(/\s+/g, ' ')
        .trim();
    if (!s) return '';
    for (let i = 11; i < s.length; i++) {
        const ch = s[i];
        if ((ch === '.' || ch === '!' || ch === '?') && (i + 1 >= s.length || s[i + 1] === ' ')) {
            s = s.slice(0, i + 1);
            break;
        }
    }
    if (s.length > maxLen) s = s.slice(0, maxLen - 1).trimEnd() + '…';
    return s;
}

/**
 * Per-type event counts for the collapsed-turn badge.
 */
export interface TurnEventCounts {
    thoughts: number;
    texts: number;
    tools: number;
}

export function countEvents(turn: AssistantTurn): TurnEventCounts {
    let thoughts = 0;
    let texts = 0;
    let tools = 0;
    for (const e of turn.events) {
        if (isThought(e)) thoughts++;
        else if (isText(e)) texts++;
        else if (isToolCall(e)) tools++;
        // compaction markers don't count toward the badge
    }
    return {thoughts, texts, tools};
}

// ---------------------------------------------------------------------------
// Event grouping (render-time, Slice 3 / Phase 2 of session_turn_rendering)
// ---------------------------------------------------------------------------

/** An event that may be folded away into a summary chip. */
export type FoldableEvent = ToolCallEvent | ThoughtEvent;

/**
 * A view-time grouping of a turn's events, built around the *live edge*: the
 * work happening right now stays visible as cards, and everything already
 * finished collapses into one chip.
 *
 * This supersedes the earlier "runs of 4+ consecutive tool calls fold" rule,
 * which barely helped in practice — a thought between two tool batches broke
 * the run, so a turn that alternated thought/tools/thought/tools rendered as a
 * dozen separate rows even though every run was individually folded. Thoughts
 * are foldable here, and a run is only broken by text, so the whole lead-up
 * collapses to a single chip.
 */
export type EventGroup =
    | {kind: 'single'; id: string; event: TurnEvent}
    | {kind: 'folded'; id: string; events: FoldableEvent[]}
    /**
     * A fan-out: contiguous `create_job` calls from one turn, rendered as
     * one card with a row per job instead of N stacked cards. Client-side only —
     * there is no `batch_id` and no backend concept behind this.
     */
    | {kind: 'job_batch'; id: string; events: ToolCallEvent[]}
    /**
     * A subagent fan-out: two or more `delegate_agent` calls issued in one
     * assistant message, rendered as one card with a row per child. Grouped by
     * `messageKey`, not by contiguity alone — two consecutive single-call
     * delegations are two ordinary cards.
     */
    | {kind: 'delegate_batch'; id: string; events: ToolCallEvent[]};

/**
 * Below this many foldable events in a row, render them as plain cards instead
 * of a chip. A "1× thought" chip is the same height as the thought card it
 * replaces and strictly less informative, so folding one event never pays.
 */
export const MIN_FOLD_RUN = 2;

/**
 * Below this many contiguous job calls, render them as ordinary cards. One job
 * in a "batch" is just a card with a redundant header.
 */
export const MIN_JOB_BATCH = 2;

/**
 * Below this many `delegate_agent` calls in one assistant message, render them
 * as ordinary cards. A single delegation keeps today's card and fold behaviour
 * exactly.
 */
export const MIN_DELEGATE_BATCH = 2;

export function isFoldable(e: TurnEvent): e is FoldableEvent {
    if (e.kind === 'thought') return true;
    if (e.kind !== 'tool_call') return false;
    // notify_user is a message addressed to the user, not work — like text,
    // it never disappears into a "N× tool calls" chip.
    //
    // create_job is excluded for a stronger reason: its card is a live
    // handle with Approve / Continue-with-feedback / Cancel on it, so folding
    // would hide work that is *waiting on the user* behind a counter. Before
    // this, a three-job fan-out rendered as a "2× tool calls" chip plus one
    // inline card, because pinnedEventIds() pins only the turn's LAST call —
    // two of the three job cards were invisible until you expanded a chip that
    // gave no hint they were there. They group into a job_batch instead.
    return e.tool !== NOTIFY_USER_TOOL && e.tool !== JOB_TOOL;
}

/** A `create_job` call — the only event that forms a `job_batch`. */
export function isJobCall(e: TurnEvent): e is ToolCallEvent {
    return e.kind === 'tool_call' && e.tool === JOB_TOOL;
}

/** A `delegate_agent` call — the only event that forms a `delegate_batch`. */
export function isDelegateCall(e: TurnEvent): e is ToolCallEvent {
    return e.kind === 'tool_call' && e.tool === DELEGATE_TOOL;
}

/**
 * Ids of the `delegate_agent` calls that belong to a fan-out: a contiguous run
 * of at least {@link MIN_DELEGATE_BATCH} such calls sharing one `messageKey`.
 *
 * Computed on the raw event list, before folding, because a batched call must
 * not fold: pinnedEventIds() keeps only the turn's last call once a batch
 * finishes, which would bury N−1 children in a "N× delegations" chip — the
 * failure the job batch was built to fix. A call outside a batch stays
 * foldable, so a single delegation renders exactly as before.
 *
 * A session rejects a batch that mixes `delegate_agent` with other tools, so
 * the calls of one fan-out are contiguous; anything between two calls breaks
 * the run.
 */
function delegateBatchIds(events: TurnEvent[]): Set<string> {
    const ids = new Set<string>();
    let run: ToolCallEvent[] = [];
    const flush = () => {
        if (run.length >= MIN_DELEGATE_BATCH) for (const e of run) ids.add(e.id);
        run = [];
    };
    for (const e of events) {
        if (!isDelegateCall(e)) {
            flush();
            continue;
        }
        if (run.length > 0 && run[run.length - 1].messageKey !== e.messageKey) flush();
        run.push(e);
    }
    flush();
    return ids;
}

/**
 * Whether assistant prose follows event `eventId` of `turn` anywhere later in
 * the transcript: in the same turn after it, or in any later assistant turn.
 *
 * Prose after a tool call means the model was asked again, so that call's
 * turn has moved on. For a call still without a result this separates the
 * two cases history cannot tell apart by status: a fan-out still running
 * (nothing follows it yet) and one a crash abandoned (a recovery answered
 * after it). User and system rows do not count — input typed during a batch
 * and a recovery notice both arrive while the batch may still be running.
 */
export function isFollowedByAnswer(
    turns: readonly Turn[],
    turn: AssistantTurn,
    eventId: string,
): boolean {
    const at = turns.indexOf(turn);
    if (at < 0) return false;
    const from = turn.events.findIndex((e) => e.id === eventId);
    if (from >= 0) {
        for (let i = from + 1; i < turn.events.length; i++) {
            if (isText(turn.events[i])) return true;
        }
    }
    for (let t = at + 1; t < turns.length; t++) {
        const later = turns[t];
        if (isAssistantTurn(later) && later.events.some(isText)) return true;
    }
    return false;
}

/**
 * Whether this event is still in flight. Tool results can land out of order, so
 * this is a per-event property, not "is it near the end of the array".
 */
export function isEventInFlight(e: TurnEvent): boolean {
    if (e.kind === 'tool_call') return e.status === 'pending' || e.status === 'running';
    if (e.kind === 'thought') return e.status === 'streaming';
    return false;
}

/**
 * The events that must stay visible as cards:
 *   - anything in flight — the whole point is watching work happen. Fire five
 *     tools at once and all five show; each drops into the chip as it returns.
 *   - the turn's most recent tool call, always, so a finished turn still shows
 *     what it last did rather than collapsing to a bare counter.
 */
export function pinnedEventIds(events: TurnEvent[]): Set<string> {
    const pinned = new Set<string>();
    for (const e of events) {
        if (isEventInFlight(e)) pinned.add(e.id);
    }
    for (let i = events.length - 1; i >= 0; i--) {
        if (isToolCall(events[i])) {
            pinned.add(events[i].id);
            break;
        }
    }
    return pinned;
}

/**
 * Partition a turn's flat event list into render groups.
 *
 * Text and compaction never fold — text is the answer, and a compaction marker
 * is too significant to hide. Every other event folds unless it's pinned, and
 * contiguous unpinned stretches merge into one chip. Order is preserved, so a
 * completed call sandwiched between two in-flight ones simply renders inline.
 * Job fan-outs and subagent fan-outs never fold; each becomes one batch group.
 *
 * A group's `id` (= its first member's event id, stable across SSE replay) is
 * suitable for `@for (… ; track group.id)`.
 */
export function groupEvents(events: TurnEvent[]): EventGroup[] {
    const pinned = pinnedEventIds(events);
    const delegateBatched = delegateBatchIds(events);
    const groups: EventGroup[] = [];
    let run: FoldableEvent[] = [];
    const flush = () => {
        if (run.length === 0) return;
        if (run.length < MIN_FOLD_RUN) {
            for (const e of run) groups.push({kind: 'single', id: e.id, event: e});
        } else {
            groups.push({kind: 'folded', id: run[0].id, events: run});
        }
        run = [];
    };
    for (const e of events) {
        if (isFoldable(e) && !pinned.has(e.id) && !delegateBatched.has(e.id)) {
            run.push(e);
        } else {
            flush();
            groups.push({kind: 'single', id: e.id, event: e});
        }
    }
    flush();
    const jobs = batchRuns(groups, 'job_batch', isJobCall, MIN_JOB_BATCH);
    if (delegateBatched.size === 0) return jobs;
    return batchRuns(
        jobs,
        'delegate_batch',
        (e): e is ToolCallEvent => delegateBatched.has(e.id) && isDelegateCall(e),
        MIN_DELEGATE_BATCH,
        (prev, next) => prev.messageKey === next.messageKey,
    );
}

/**
 * Merge contiguous member singles into one batch group (`job_batch` for
 * `create_job`, `delegate_batch` for `delegate_agent`).
 *
 * A post-pass over the finished groups rather than a branch inside the loop
 * above, because it must merge *across* whatever the fold pass produced while
 * preserving order exactly: a member is never folded, so it always arrives
 * here as its own `single`, and anything between two members (text, a folded
 * chip, a thought) breaks the run — which is what you want, since it means the
 * agent said or did something between the two dispatches. `sameRun` splits a
 * run further; the delegate batch uses it to keep two back-to-back messages
 * apart.
 */
function batchRuns(
    groups: EventGroup[],
    kind: 'job_batch' | 'delegate_batch',
    isMember: (e: TurnEvent) => e is ToolCallEvent,
    minSize: number,
    sameRun: (prev: ToolCallEvent, next: ToolCallEvent) => boolean = () => true,
): EventGroup[] {
    if (!groups.some((g) => g.kind === 'single' && isMember(g.event))) return groups;
    const out: EventGroup[] = [];
    let run: ToolCallEvent[] = [];
    const flush = () => {
        if (run.length === 0) return;
        if (run.length < minSize) {
            for (const e of run) out.push({kind: 'single', id: e.id, event: e});
        } else {
            out.push({kind, id: run[0].id, events: run});
        }
        run = [];
    };
    for (const g of groups) {
        if (g.kind === 'single' && isMember(g.event)) {
            if (run.length > 0 && !sameRun(run[run.length - 1], g.event)) flush();
            run.push(g.event);
        } else {
            flush();
            out.push(g);
        }
    }
    flush();
    return out;
}

/**
 * Category-count summary for a folded chip: "24× searches · 20× citations ·
 * 6× thoughts". Counting by category rather than by type is deliberate — a
 * total plus per-category counts double-counts, because commands *are* tool
 * calls. Thoughts get their own bucket.
 *
 * Category resolution is left to the caller (it needs i18n); this returns raw
 * category keys with `'thought'` for reasoning and `'other'` for tool calls
 * whose category is absent (older history rows).
 */
export interface FoldedSummaryPart {
    /** Tool category key, or the literal 'thought' / 'other'. */
    category: string;
    count: number;
}

export interface FoldedSummary {
    parts: FoldedSummaryPart[];
    /** Calls that errored or were denied — surfaced as a chip badge. */
    failed: number;
}

export function summarizeFolded(events: FoldableEvent[]): FoldedSummary {
    const counts = new Map<string, number>();
    let failed = 0;
    for (const e of events) {
        let key: string;
        if (e.kind === 'thought') {
            key = 'thought';
        } else {
            key = e.category || 'other';
            if (e.status === 'error' || e.status === 'denied' || e.resultStatus === 'error') failed++;
        }
        counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    // Highest count first; ties keep insertion (= first-appearance) order, which
    // Map iteration gives us for free.
    const parts = [...counts.entries()]
        .map(([category, count]) => ({category, count}))
        .sort((a, b) => b.count - a.count);
    return {parts, failed};
}

// =============================================================================
// Officer log lens (officer_visibility_streamline.md §3.2)
// =============================================================================

/** Every officer wake opens with a server-computed `[SITREP]` system turn. */
export const SITREP_PREFIX = '[SITREP]';

export function isSitrepTurn(t: Turn): t is SystemTurn {
    return t.kind === 'system' && t.content.trimStart().startsWith(SITREP_PREFIX);
}

/**
 * A quiet wake: the officer thought, maybe said a line, and filed his sleep.
 * Any other tool call — a dispatch, a steer, a `notify_user`, a worker
 * reply — makes the wake worth reading, so it stays expanded.
 */
export function isQuietWakeTurn(t: Turn): t is AssistantTurn {
    if (t.kind !== 'assistant' || t.status !== 'done') return false;
    let slept = false;
    for (const e of t.events) {
        if (e.kind !== 'tool_call') continue;
        if (e.tool !== SLEEP_TOOL || e.status !== 'completed') return false;
        slept = true;
    }
    return slept;
}

/** The last sleep call's request, for the folded line. */
export function sleepRequest(turn: AssistantTurn): {minutes: number | null; reason: string} {
    let call: ToolCallEvent | undefined;
    for (const e of turn.events) {
        if (e.kind === 'tool_call' && e.tool === SLEEP_TOOL) call = e;
    }
    const args = call?.args ?? {};
    const raw = args['minutes'];
    const minutes = typeof raw === 'number' ? raw : raw == null ? NaN : Number(raw);
    return {
        minutes: Number.isFinite(minutes) ? minutes : null,
        reason: typeof args['reason'] === 'string' ? args['reason'] : '',
    };
}

/** A sitrep and the quiet wake it produced, folded into one line. */
export interface WakeCycle {
    kind: 'wake_cycle';
    /** The sitrep turn's id — stable across re-folds. */
    id: string;
    sitrep: SystemTurn;
    wake: AssistantTurn;
    minutes: number | null;
    reason: string;
}

export type TurnView = {kind: 'turn'; id: string; turn: Turn} | WakeCycle;

/**
 * Fold each `[SITREP]` + quiet-wake pair into a `WakeCycle`; every other turn
 * passes through untouched and in order. Pure and cheap — safe in a computed.
 */
export function foldWakeCycles(turns: readonly Turn[]): TurnView[] {
    const out: TurnView[] = [];
    for (let i = 0; i < turns.length; i++) {
        const t = turns[i];
        const next = turns[i + 1];
        if (isSitrepTurn(t) && next !== undefined && isQuietWakeTurn(next)) {
            const {minutes, reason} = sleepRequest(next);
            out.push({kind: 'wake_cycle', id: t.id, sitrep: t, wake: next, minutes, reason});
            i++;
            continue;
        }
        out.push({kind: 'turn', id: t.id, turn: t});
    }
    return out;
}

/** The session-reload boundary: `turn` was loaded from history and `next` was not. */
export function isSessionBoundary(turn: Turn, next: Turn | undefined): boolean {
    if (!next) return false;
    const turnHistorical = (turn.kind === 'assistant' || turn.kind === 'user') && !!turn.historical;
    const nextHistorical = (next.kind === 'assistant' || next.kind === 'user') && !!next.historical;
    return turnHistorical && !nextHistorical;
}

/** The last underlying turn a view renders (a cycle ends with its wake). */
export function lastTurnOf(view: TurnView): Turn {
    return view.kind === 'wake_cycle' ? view.wake : view.turn;
}

/**
 * The next turn that speaks — assistant or user — after view `index`, walking
 * each later view's turns in render order. System and compaction rows never
 * mark a session boundary: a history-loaded sitrep between two history wakes
 * is not a reload.
 */
export function nextSpeakingTurn(views: readonly TurnView[], index: number): Turn | undefined {
    for (let i = index + 1; i < views.length; i++) {
        const v = views[i];
        const turns = v.kind === 'wake_cycle' ? [v.sitrep, v.wake] : [v.turn];
        for (const t of turns) {
            if (t.kind === 'assistant' || t.kind === 'user') return t;
        }
    }
    return undefined;
}
