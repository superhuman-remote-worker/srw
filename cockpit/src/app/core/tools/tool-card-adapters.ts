import {ToolCallEvent} from '../models/turn.model';
import {AuditEntry} from '../models/audit.model';
import {recoveredDelegateCallOutcome} from '../models/delegate-batch.model';
import {NormalizedToolCall, ToolCardStatus, ToolCardView} from '../models/tool-card.model';
import {buildToolCardView} from './tool-descriptors';

/**
 * Thin source-specific adapters that normalize each tool-call shape into a
 * {@link NormalizedToolCall}, then delegate to the shared
 * {@link buildToolCardView}. All tool semantics live in the descriptor
 * registry — these only translate field names and map status.
 *
 * Design: `knowledge-base/knowledge/features/unified_tool_cards.md`.
 */

/** Live persistent-session statuses → unified card status. */
function mapEventStatus(s: ToolCallEvent['status']): ToolCardStatus {
    switch (s) {
        case 'completed':
            return 'ok';
        case 'running':
        case 'pending':
        case 'denied':
        case 'error':
        case 'expired':
            return s;
        default:
            return 'pending';
    }
}

/** Live WS / history `ToolCallEvent` → unified view-model. */
export function toolCardViewFromEvent(tc: ToolCallEvent): ToolCardView {
    const normalized: NormalizedToolCall = {
        tool: tc.tool,
        args: tc.args || {},
        status: mapEventStatus(tc.status),
        // The live result string is the full content the model saw (incl. the
        // error output on a failed call — surfaced tinted, not duplicated).
        result: tc.result,
        durationMs: tc.durationMs,
        exitCode: tc.exitCode,
    };
    const view = buildToolCardView(normalized);
    // A delegation a recovery settled says what became of the child.
    const outcome = recoveredDelegateCallOutcome(tc);
    return outcome ? {...view, outcome} : view;
}

/** Workbench MongoDB audit `AuditEntry` (a tool step) → unified view-model. */
export function toolCardViewFromAudit(entry: AuditEntry): ToolCardView {
    const tool = entry.tool;
    // completed_at === null (with a started_at) means the call is still running.
    const pending = entry.started_at !== undefined && entry.completed_at === null;
    let status: ToolCardStatus;
    if (pending) {
        status = 'running';
    } else if (tool?.success === false || tool?.error) {
        status = 'error';
    } else {
        status = 'ok';
    }
    const normalized: NormalizedToolCall = {
        tool: tool?.name || '<unknown>',
        args: tool?.arguments || {},
        status,
        // The audit trail stores only a preview; surface the full size so the
        // card can note "preview · N total".
        result: tool?.result_preview ?? undefined,
        resultBytesTotal: tool?.result_size_bytes ?? undefined,
        error: tool?.error ?? undefined,
        durationMs: entry.latency_ms,
    };
    return buildToolCardView(normalized);
}
