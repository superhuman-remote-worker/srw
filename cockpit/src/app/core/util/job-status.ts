import {BadgeTone} from '../../ui/badge/badge.component';

/**
 * Shared job-status vocabulary.
 *
 * `jobStatusTone` was copy-pasted verbatim into `job-list.component.ts` and
 * `job-review.component.ts`; the job tool card is the third consumer and the
 * point at which duplicating it again stops being defensible. Both call sites
 * now delegate here.
 *
 * `isTerminalJobStatus` is new — no terminal-status predicate existed anywhere
 * in the frontend, and a card that polls for live status needs one to know when
 * to stop. Getting it wrong is not cosmetic: too narrow and a finished job is
 * polled forever, too broad and the card freezes mid-run.
 *
 * Design: knowledge-base/knowledge/features/unified_tool_cards.md (slice 4).
 */

/**
 * Statuses from which a job never moves again on its own.
 *
 * `pending_review` is deliberately NOT terminal: the job stopped, but an
 * approval flips it to `completed`, and that transition is exactly what a
 * watching card must catch. (The same asymmetry drives the wake feature's
 * per-status dedup key — see knowledge-base/knowledge/features/session_wake_on_job_completion.md.)
 *
 * `paused` is also not terminal — the dispatcher re-picks a paused job.
 */
const TERMINAL_JOB_STATUSES: ReadonlySet<string> = new Set([
    'completed',
    'failed',
    'cancelled',
    'blocked_undelivered',
]);

export function isTerminalJobStatus(status: string | null | undefined): boolean {
    return !!status && TERMINAL_JOB_STATUSES.has(status);
}

/** True while a job is still doing work (as opposed to waiting on a human). */
export function isRunningJobStatus(status: string | null | undefined): boolean {
    return status === 'created' || status === 'processing' || status === 'waiting'
        || status === 'reviewing' || status === 'paused';
}

/**
 * Statuses from which a human can hand the job back with guidance.
 *
 * The rule is "stopped, and will not restart itself": the job is waiting on a
 * person, so replying to it is meaningful.
 *
 * Narrower than the server, deliberately. The server accepts stopped jobs but
 * refuses completed and blocked/undelivered outcomes. Several other statuses
 * would still be wrong to offer in a transcript card:
 *
 * - **`paused`** is dispatchable-and-unassigned — the dispatcher re-picks it on
 *   its own, and the card is already showing a spinner for it
 *   ({@link isRunningJobStatus}). A "continue" button under a spinner reads as
 *   broken.
 * - **`processing`/`created`/`waiting`/`reviewing`** are live; the job has not
 *   asked for anything.
 *
 * So: `pending_review` (the frozen-for-review case this exists for), plus
 * `failed` and `cancelled` (retry with guidance) — the same set the Jobs list
 * offers plain Resume on, minus `paused` and `created`.
 */
export function canResumeJobStatus(status: string | null | undefined): boolean {
    return status === 'pending_review' || status === 'failed' || status === 'cancelled';
}

export interface JobOutcomeView {
    status?: string | null;
    completion_outcome_kind?: string | null;
    workspace_recovery?: {state?: string | null} | null;
}

/** Presentation status for a job whose storage status stays rolling-safe. */
export function effectiveJobStatus(job: JobOutcomeView | null | undefined): string {
    if (job?.workspace_recovery) {
        return job.workspace_recovery.state === 'paused_attention'
            ? 'recovery_paused'
            : 'recovering_workspace';
    }
    return job?.completion_outcome_kind === 'blocked_undelivered'
        ? 'blocked_undelivered'
        : (job?.status ?? '');
}

/** A blocked/undelivered terminal outcome is intentionally not resumable. */
export function canResumeJob(job: JobOutcomeView | null | undefined): boolean {
    return job?.completion_outcome_kind !== 'blocked_undelivered'
        && canResumeJobStatus(job?.status);
}

/**
 * Coerce a job's JSONB-backed field into an object.
 *
 * `GET /api/jobs/{id}` returns `context` and `freeze_data` as raw JSON
 * **strings**, not objects — asyncpg hands JSONB back as text and the
 * orchestrator passes it through. The cockpit `Job` model nonetheless types
 * them as `Record<string, any>`, so indexing straight into one type-checks,
 * compiles, and silently yields `undefined` forever. Verified against a real
 * dev job on 2026-07-29.
 *
 * Returns null for anything that isn't a usable object.
 */
export function asRecord(value: unknown): Record<string, unknown> | null {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
        return value as Record<string, unknown>;
    }
    if (typeof value === 'string' && value.trim()) {
        try {
            const parsed: unknown = JSON.parse(value);
            if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                return parsed as Record<string, unknown>;
            }
        } catch {
            return null;
        }
    }
    return null;
}

/**
 * Job statuses that have a `jobs.status.*` label in the locale files.
 *
 * Mirrors the `valid_status` CHECK constraint on `jobs`
 * (`0001_initial.sql:557`) — keep the two in step, and add the locale entry in
 * BOTH `en.json` and `de-DE.json` when the server gains a status, or the badge
 * silently degrades to the raw enum.
 *
 * Exists so the label can be resolved by the `transloco` pipe (which handles
 * catalogue loading and language switches) while still falling back to the raw
 * value for an unknown status. Resolving it with `TranslocoService.translate()`
 * inside a `computed()` looks simpler and is wrong: the call emits a transloco
 * event, and emitting during template evaluation trips NG0600.
 */
const LABELLED_JOB_STATUSES: ReadonlySet<string> = new Set([
    'created', 'processing', 'completed', 'failed', 'cancelled',
    'pending_review', 'paused', 'reviewing', 'waiting', 'waiting_for_reply',
    'blocked_undelivered',
    'recovering_workspace', 'recovery_paused',
]);

/** i18n key for a job status, or null when it has no label to fall back from. */
export function jobStatusLabelKey(status: string | null | undefined): string | null {
    return status && LABELLED_JOB_STATUSES.has(status) ? `jobs.status.${status}` : null;
}

/** Badge tone for a job status. Single source of truth for all three surfaces. */
export function jobStatusTone(status: string): BadgeTone {
    switch (status) {
        case 'completed':
            return 'success';
        case 'processing':
        case 'recovering_workspace':
        case 'pending_review':
        case 'blocked_undelivered':
            return 'warning';
        case 'failed':
        case 'recovery_paused':
            return 'danger';
        case 'created':
        case 'waiting':
            return 'info';
        case 'reviewing':
            return 'accent';
        case 'cancelled':
        case 'paused':
            return 'neutral';
        default:
            return 'neutral';
    }
}
