import type {BadgeTone} from '../../ui/badge/badge.component';
import {DiffLine} from '../util/line-diff';

/**
 * Unified, source-agnostic view-model for an agent tool call.
 *
 * Both the live persistent-session stream (`ToolCallEvent`) and the workbench
 * MongoDB audit trail (`AuditToolInfo`) map into this one shape, which a single
 * presentational component (`<app-tool-card>`) renders identically. All
 * per-tool knowledge (title, icon, which arg is the "primary" parameter, how to
 * render the result) lives in the descriptor registry — see
 * `core/tools/tool-descriptors.ts`. Design: `knowledge-base/knowledge/features/unified_tool_cards.md`.
 */

export type ToolCardStatus = 'pending' | 'running' | 'ok' | 'error' | 'denied' | 'expired';

/**
 * An outcome the six {@link ToolCardStatus} values cannot state, shown in the
 * card's status pill in their place. Today only a lone `delegate_agent` call
 * whose result a delegation-batch recovery wrote: interrupted, not started,
 * cancelled — with the label and tone the same call has as a batch-card row
 * (`recoveredDelegateCallOutcome` in `delegate-batch.model.ts`). `status`
 * stays the call's own, so everything else about the card is unchanged.
 */
export interface ToolCardOutcome {
    /** Transloco key of the pill label. */
    readonly labelKey: string;
    readonly tone: BadgeTone;
}

/** How a result/param body should be presented. */
export type ToolResultKind =
    | 'text'
    | 'code'
    | 'terminal'
    | 'diff'
    | 'json'
    | 'markdown'
    | 'none';

export type ParamKind = 'code' | 'path' | 'text' | 'json';

/** A single input parameter, shown full (never truncated) in the expanded body. */
export interface ToolParam {
    label: string;
    value: string;
    kind: ParamKind;
}

/** A small meta fact (duration, exit code, size). Omitted as a group when empty. */
export interface ToolDetail {
    label: string;
    value: string;
    tone?: 'default' | 'ok' | 'error' | 'warn';
}

/** The result the model saw, rendered per `kind`. */
export interface ToolResult {
    kind: ToolResultKind;
    /** Plain content for text/code/terminal/json/markdown kinds. */
    content?: string;
    /** Highlight hint for `code`, derived from a file extension when available. */
    language?: string;
    /** Diff lines for `kind === 'diff'`. */
    diffLines?: DiffLine[];
    diffMode?: 'replace' | 'append' | 'prepend' | 'write';
    /** Lines dropped by the adapter's render cap (diff), surfaced as a footer. */
    truncatedLines?: number;
    /** Set when `content` is only a preview of a larger result (workbench side). */
    bytesTotal?: number;
}

/**
 * A durable thing a card is a *handle on*, as opposed to a record of a call
 * that already finished.
 *
 * Every card in slices 1–3 is a snapshot: the tool ran, it returned, the card
 * shows what the model saw. A job card is different — the call returns in
 * milliseconds ("job created, id=…") while the thing the user cares about runs
 * for twenty minutes afterwards, changes status several times, and then wants
 * acting on. `entity` is what turns a transcript artifact into a view onto a
 * row that keeps changing, and it generalizes (a future `create_project` card
 * would use the same seam).
 *
 * Absent on every card that exists today, which stay inert.
 *
 * NOTE: the live status is deliberately NOT stored here. `ToolCardView` is
 * memoized per event object in an identity-keyed WeakMap, so writing status
 * into it would mean busting that cache on every poll. Live state lives in a
 * separate signal map — see `JobWatchService`.
 */
export interface ToolCardEntity {
    readonly kind: 'job';
    readonly id: string;
}

export interface ToolCardAction {
    readonly kind: 'open_canvas';
    /** Revision presented by this historical tool result when recoverable. */
    readonly presentationRevision?: number;
    /**
     * Content hash presented by this historical tool result, when recoverable.
     *
     * Preferred over `presentationRevision` for deciding whether a card is
     * still current: the revision advances for reasons that do not change what
     * is on stage — notably re-binding a Canvas to a rebuilt workspace, which
     * bumps it while presenting the identical bytes.
     */
    readonly sourceVersion?: string;
}

export type CanvasPresentationKind = 'file' | 'app' | 'canvas';

/** Bounded display metadata extracted from a successful trusted Canvas result. */
export interface CanvasPresentationSummary {
    readonly sourceKind: CanvasPresentationKind;
    readonly title?: string;
}

/**
 * The officer tool whose calls are a message TO the user, not work. Shared by
 * the card builder (bubble view) and the turn fold logic (never folded into a
 * "N× tool calls" chip — a message addressed to the user is first-class, like
 * text).
 */
export const NOTIFY_USER_TOOL = 'notify_user';

/** The officer's park verb (src/tools/core/officer.py); args `{minutes, reason}`. */
export const SLEEP_TOOL = 'sleep';

/**
 * The fleet tool whose calls are a durable, *actionable* handle rather than a
 * record of finished work. Shared by the card descriptor, the id parser and the
 * turn fold logic.
 *
 * Never folded into a "N× tool calls" chip, for the same reason as
 * {@link NOTIFY_USER_TOOL} but a stronger one: the card carries live status and
 * the Approve / Continue-with-feedback / Cancel buttons, so hiding it behind a
 * counter hides work that is *waiting on the user*. Contiguous calls group into
 * a `job_batch` instead — see `groupEvents` in `core/models/turn.model.ts`.
 */
export const JOB_TOOL = 'create_job';

/**
 * The session tool that runs a roster subagent. Two or more calls carried by
 * one assistant message are a fan-out and group into a `delegate_batch` — see
 * `groupEvents` in `core/models/turn.model.ts`. A lone call keeps the ordinary
 * card and folds like any other finished tool call.
 */
export const DELEGATE_TOOL = 'delegate_agent';

export type NotifyUrgency = 'log' | 'digest' | 'page';

/**
 * First-class officer→user message extracted from a `notify_user` call.
 * Present only on that tool; its presence switches `<app-tool-card>` from the
 * generic collapsed card to a chat-bubble rendering (the officer is
 * *addressing* the user, so the call's args ARE the message).
 */
export interface NotifyMessageView {
    readonly urgency: NotifyUrgency;
    /** Short subject line; page/digest only in practice. */
    readonly subject?: string;
    /** The message body, rendered like normal chat message text. */
    readonly body: string;
    /** Delivery receipt — the tool's result string ("Paged the Legate (2/3 …)."). */
    readonly receipt?: string;
}

/**
 * The complete, render-ready description of one tool call. Produced by
 * `buildToolCardView()` from a `NormalizedToolCall`; consumed by
 * `<app-tool-card [view]>`.
 */
export interface ToolCardView {
    /** Raw tool id, e.g. `run_command`. Shown small/mono in the body. */
    tool: string;
    /** Human verb, e.g. "Execute command". May be an i18n key (literal fallback). */
    title: string;
    /** Material-symbol icon name. */
    icon: string;
    /** Short, single-line, ellipsized collapsed-row hint. May be empty. */
    subtitle?: string;
    status: ToolCardStatus;
    /** Replaces the pill's status label and tone when set; see {@link ToolCardOutcome}. */
    outcome?: ToolCardOutcome;
    params: ToolParam[];
    result?: ToolResult;
    details: ToolDetail[];
    /** Optional trusted Cockpit action; never an agent-supplied URL. */
    action?: ToolCardAction;
    /**
     * Durable row this card watches, when it has one. Present only on
     * `create_job` today. Its mere presence is the subscription — the
     * card looks the id up in the live map and renders current state instead of
     * the frozen call result.
     */
    entity?: ToolCardEntity;
    /** Normalized type/title only; never includes source URLs or workspace internals. */
    canvasPresentation?: CanvasPresentationSummary;
    /** Officer→user message (notify_user only) — renders as a bubble, not a card. */
    notify?: NotifyMessageView;
    /** Explicit error message (audit) or the errored output (live). */
    error?: string;
}

/**
 * Minimal common shape each data source normalizes into before
 * `buildToolCardView()` applies descriptor-driven rendering. Keeps all tool
 * semantics in one place; the per-source adapters only translate field names
 * and map status.
 */
export interface NormalizedToolCall {
    tool: string;
    args: Record<string, unknown>;
    status: ToolCardStatus;
    /** Content the model saw. May be a preview (see `resultBytesTotal`). */
    result?: string | null;
    resultBytesTotal?: number | null;
    error?: string | null;
    durationMs?: number | null;
    exitCode?: number | null;
}
