import {
    AssistantTurn,
    CompactionEvent,
    ConversationState,
    EMPTY_CONVERSATION,
    TextEvent,
    ThoughtEvent,
    ToolCallEvent,
    Turn,
    TurnEvent,
} from '../models/turn.model';
import {ChatAttachment} from './persistent-chat.service';

/**
 * Pure reducer for ConversationState.
 *
 * One assistant "turn" in the UI is one bubble; internally it's an ordered
 * list of typed events (thoughts, plain-text blocks, tool calls). The
 * reducer maps SSE wire events to event-list mutations.
 *
 * Idempotency: every mutation is keyed by stable ids (turnId,
 * `${turnId}.b<index>`, or backend tool_use_id), so replaying the same
 * event sequence converges to the same final state. SSE replay via
 * `Last-Event-ID` cursor is therefore safe.
 *
 * ThoughtEvent boundary rule: each `thinking` action is a delta. If the
 * most recent event in the active turn is an open ThoughtEvent, the delta
 * appends. Otherwise (the previous event was non-thinking, or there's no
 * prior thought yet), a new ThoughtEvent opens. This produces correct
 * boundaries for both interleaved-thinking models (think → tool → think →
 * tool) and traditional-thinking-per-API-turn models, without requiring
 * server-side markers.
 */

export type ReducerAction =
    | { type: 'reset'; threadId?: string | null }
    | { type: 'load_history'; threadId: string; turns: Turn[]; preserveLive?: boolean }
    | {
        type: 'user_message';
        id: string;
        content: string;
        attachments?: ChatAttachment[];
        timestamp: number;
    }
    | {
        type: 'system_message';
        id: string;
        content: string;
        timestamp: number;
    }
    | { type: 'reattach_turn'; turnId: string; timestamp: number }
    | { type: 'turn_started'; turnId: string; startedAt: number; model?: string }
    | { type: 'turn_completed'; turnId: string; finishedAt: number }
    | { type: 'turn_interrupted'; turnId: string; finishedAt: number }
    | { type: 'token'; content: string; timestamp: number }
    | { type: 'thinking'; content: string; timestamp: number; messageId?: string }
    | { type: 'thinking_reset'; messageId?: string; timestamp: number }
    | {
        type: 'tool_started';
        toolUseId: string;
        tool: string;
        args: Record<string, unknown>;
        category?: string;
        timestamp: number;
    }
    | {
        type: 'tool_completed';
        toolUseId: string;
        result?: string;
        isError?: boolean;
        timestamp: number;
    }
    | {
        type: 'permission_request';
        toolUseId: string;
        tool: string;
        args: Record<string, unknown>;
        timestamp: number;
    }
    | {
        type: 'permission_decision';
        toolUseId: string;
        decision: 'approved' | 'denied' | 'expired';
        timestamp: number;
    }
    | { type: 'remove_turn'; id: string }
    | { type: 'update_attachments'; id: string; attachments: ChatAttachment[] }
    | { type: 'add_compaction'; id: string; summary: string; timestamp: number };

export function reduce(state: ConversationState, action: ReducerAction): ConversationState {
    switch (action.type) {
        case 'reset':
            return {...EMPTY_CONVERSATION, threadId: action.threadId ?? null};

        case 'load_history':
            if (action.preserveLive) {
                const historyIds = new Set(action.turns.map((turn) => turn.id));
                const live = state.turns.filter(
                    (turn) =>
                        (!("historical" in turn) || turn.historical !== true) &&
                        !historyIds.has(turn.id),
                );
                return {
                    threadId: action.threadId,
                    turns: [...action.turns, ...live],
                    activeAssistantTurnId:
                        live.some((turn) => turn.id === state.activeAssistantTurnId)
                            ? state.activeAssistantTurnId
                            : null,
                };
            }
            return {
                threadId: action.threadId,
                turns: action.turns,
                activeAssistantTurnId: null,
            };

        case 'user_message':
            return {
                ...state,
                turns: [
                    ...state.turns,
                    {
                        kind: 'user',
                        id: action.id,
                        content: action.content,
                        attachments: action.attachments,
                        timestamp: action.timestamp,
                    },
                ],
            };

        case 'update_attachments':
            // Re-key an already-rendered user bubble's chips as its uploads
            // resolve: `path` appears, the server may have renamed a file
            // (`_1` collision suffix), and one .zip expands into several
            // chips. Patching in place is the only correct shape — a second
            // `user_message` with the same id would APPEND a duplicate
            // bubble, and remove+re-add would move it to the foot of the
            // transcript, below messages the user queued behind it.
            return {
                ...state,
                turns: state.turns.map((t) =>
                    t.kind === 'user' && t.id === action.id
                        ? {...t, attachments: action.attachments}
                        : t,
                ),
            };

        case 'remove_turn':
            // Roll back an optimistic turn (e.g. a user message whose POST
            // hard-failed) by id. Safe: every Turn carries a unique id and
            // activeAssistantTurnId is untouched (the removed turn is a user
            // turn, never the streaming assistant turn).
            return {
                ...state,
                turns: state.turns.filter((t) => t.id !== action.id),
            };

        case 'add_compaction': {
            // A compaction boundary banner. Idempotent by id (stable
            // `compaction-<turn>`), so SSE replay replaces rather than
            // duplicates.
            //
            // Auto-compaction fires *mid-turn*, from the agent's tool-iteration
            // loop (`_execute_turn` in src/persistent_graph.py), so the banner
            // belongs inside the open turn at the point it fired. A top-level
            // turn can't express that: activeAssistantTurnId keeps pointing at
            // the turn above, so every post-compaction thought and tool call
            // lands in that bubble and renders *above* the divider — stranding
            // the banner at the foot of the transcript, drifting further from
            // where it fired the longer the agent works. A reload then rebuilds
            // the transcript through `historyToTurns`, which places the same
            // marker inline, so the banner visibly jumped. Mirror that
            // placement live. Between turns (manual `/compact`) there is no
            // open turn and the top-level divider is still the right shape.
            const event: CompactionEvent = {
                kind: 'compaction',
                id: action.id,
                summary: action.summary,
                startedAt: action.timestamp,
            };
            // Replay can re-deliver a frame after its turn closed, so dedupe
            // against inline markers in *every* turn, not just the active one.
            const ownsInline = (t: Turn) =>
                t.kind === 'assistant' && t.events.some((e) => e.id === action.id);
            if (state.turns.some(ownsInline)) {
                return {
                    ...state,
                    turns: state.turns.map((t) =>
                        ownsInline(t)
                            ? {
                                ...(t as AssistantTurn),
                                events: (t as AssistantTurn).events.map((e) =>
                                    e.id === action.id ? event : e,
                                ),
                            }
                            : t,
                    ),
                };
            }
            if (state.activeAssistantTurnId) {
                return updateActiveTurn(state, (t) => ({...t, events: [...t.events, event]}));
            }
            const turn: Turn = {
                kind: 'compaction',
                id: action.id,
                summary: action.summary,
                timestamp: action.timestamp,
            };
            const idx = state.turns.findIndex((t) => t.id === action.id);
            if (idx >= 0) return {...state, turns: replaceAt(state.turns, idx, turn)};
            return {...state, turns: [...state.turns, turn]};
        }

        case 'system_message': {
            const turn: Turn = {
                kind: 'system',
                id: action.id,
                content: action.content,
                timestamp: action.timestamp,
            };
            const idx = state.turns.findIndex((existing) => existing.id === action.id);
            return {
                ...state,
                turns:
                    idx >= 0
                        ? replaceAt(state.turns, idx, turn)
                        : [...state.turns, turn],
            };
        }

        case 'reattach_turn':
            return reattachTurn(state, action.turnId, action.timestamp);

        case 'turn_started': {
            // Defensive: if a prior turn is still marked active (e.g. its
            // `turn.completed` event was lost), close it before opening the
            // new one.
            let turns = state.turns;
            if (state.activeAssistantTurnId && state.activeAssistantTurnId !== action.turnId) {
                turns = turns.map((t) => {
                    if (t.kind !== 'assistant' || t.id !== state.activeAssistantTurnId) return t;
                    if (t.status !== 'streaming') return t;
                    return {
                        ...t,
                        events: closeOpenEvents(t.events, action.startedAt),
                        status: 'done' as const,
                        finishedAt: action.startedAt,
                    };
                });
            }

            // A no-cursor cold attach replays the current turn from its
            // turn.started frame. REST may already have painted an
            // incrementally persisted prefix under a message UUID; rebuild
            // that same logical turn in place from the full replay instead of
            // appending a duplicate live bubble (and duplicating its text).
            const turnNumber = numericTurnNumber(action.turnId);
            const historicalIndex =
                turnNumber === undefined
                    ? -1
                    : turns.findIndex(
                          (t) =>
                              t.kind === 'assistant' &&
                              t.id !== action.turnId &&
                              t.historical === true &&
                              t.turnNumber === turnNumber,
                      );
            if (historicalIndex >= 0) {
                const historical = turns[historicalIndex] as AssistantTurn;
                const rebuilt: AssistantTurn = {
                    ...historical,
                    id: action.turnId,
                    events: [],
                    status: 'streaming',
                    turnNumber,
                    model: action.model ?? historical.model,
                    startedAt: Math.min(historical.startedAt, action.startedAt),
                    finishedAt: undefined,
                };
                return {
                    ...state,
                    turns: replaceAt(turns, historicalIndex, rebuilt),
                    activeAssistantTurnId: action.turnId,
                };
            }

            // A replayed turn_started for a turn this tab already holds live
            // means the stream re-anchored before the turn began (a stale or
            // horizon-lost cursor) and is about to re-deliver every frame of
            // it. Rebuild the turn from that replay rather than appending a
            // second copy of its thoughts and tool calls behind the answer.
            const existing = turns.find(
                (t): t is AssistantTurn => t.kind === 'assistant' && t.id === action.turnId,
            );
            if (existing) {
                return {
                    ...state,
                    turns: turns.map((turn) =>
                        turn === existing
                            ? {
                                  ...existing,
                                  events: [],
                                  status: 'streaming',
                                  finishedAt: undefined,
                                  model: action.model ?? existing.model,
                              }
                            : turn,
                    ),
                    activeAssistantTurnId: action.turnId,
                };
            }

            const newTurn: AssistantTurn = {
                kind: 'assistant',
                id: action.turnId,
                events: [],
                status: 'streaming',
                turnNumber: numericTurnNumber(action.turnId),
                model: action.model,
                startedAt: action.startedAt,
            };
            return {
                ...state,
                turns: [...turns, newTurn],
                activeAssistantTurnId: action.turnId,
            };
        }

        case 'turn_completed':
        case 'turn_interrupted': {
            const requestedStatus =
                action.type === 'turn_interrupted' ? 'interrupted' : 'done';
            const directMatch = state.turns.some(
                (t) => t.kind === 'assistant' && t.id === action.turnId,
            );
            // Defense-in-depth: if no turn matches the real turnId but the
            // active turn is a placeholder synthesised by appendDelta /
            // updateActiveTurn (recovered === true), promote it to the real
            // id before closing — otherwise the streaming bubble would
            // hang forever (see
            // knowledge-base/knowledge/issues/persistent_chat_lost_assistant_turn_on_mid_turn_reload.md
            // §Approach 2).
            const activeId = state.activeAssistantTurnId;
            const activeTurn =
                activeId
                    ? state.turns.find(
                          (t): t is AssistantTurn =>
                              t.kind === 'assistant' && t.id === activeId,
                      )
                    : null;
            const shouldPromote =
                !directMatch && activeTurn != null && activeTurn.recovered === true;
            return {
                ...state,
                turns: state.turns.map((t) => {
                    if (t.kind !== 'assistant') return t;
                    if (t.id === action.turnId) {
                        const finalStatus =
                            requestedStatus === 'done' &&
                            t.status === 'interrupted'
                                ? 'interrupted'
                                : requestedStatus;
                        return {
                            ...t,
                            events: closeOpenEvents(t.events, action.finishedAt),
                            status: finalStatus,
                            finishedAt: action.finishedAt,
                        };
                    }
                    if (shouldPromote && t.id === activeId) {
                        return {
                            ...t,
                            id: action.turnId,
                            recovered: undefined,
                            events: closeOpenEvents(t.events, action.finishedAt),
                            status: requestedStatus,
                            finishedAt: action.finishedAt,
                        };
                    }
                    return t;
                }),
                activeAssistantTurnId:
                    state.activeAssistantTurnId === action.turnId || shouldPromote
                        ? null
                        : state.activeAssistantTurnId,
            };
        }

        case 'token':
            return appendDelta(state, 'text', action.content, action.timestamp);

        case 'thinking':
            return appendThought(state, action.content, action.timestamp, action.messageId);

        case 'thinking_reset':
            return resetThought(state, action.messageId);

        case 'tool_started':
            return updateActiveTurn(ensurePlaceholderTurn(state, action.timestamp), (turn) => {
                const closed = closeOpenEvents(turn.events, action.timestamp);
                const idx = closed.findIndex(
                    (e) => e.kind === 'tool_call' && e.id === action.toolUseId,
                );
                if (idx >= 0) {
                    // Existing entry (e.g. created by a prior permission.request) — promote.
                    const existing = closed[idx] as ToolCallEvent;
                    const updated: ToolCallEvent = {
                        ...existing,
                        tool: action.tool,
                        args: action.args,
                        category: action.category ?? existing.category,
                        status: 'running',
                        startedAt: action.timestamp,
                    };
                    return {...turn, events: replaceAt(closed, idx, updated)};
                }
                const newCall: ToolCallEvent = {
                    kind: 'tool_call',
                    id: action.toolUseId,
                    tool: action.tool,
                    args: action.args,
                    category: action.category,
                    status: 'running',
                    startedAt: action.timestamp,
                };
                return {...turn, events: [...closed, newCall]};
            });

        case 'tool_completed':
            return updateActiveTurn(ensurePlaceholderTurn(state, action.timestamp), (turn) => {
                const idx = turn.events.findIndex(
                    (e) => e.kind === 'tool_call' && e.id === action.toolUseId,
                );
                if (idx < 0) {
                    // Orphan tool_completed (replay quirk) — fabricate a finished entry.
                    const synthetic: ToolCallEvent = {
                        kind: 'tool_call',
                        id: action.toolUseId,
                        tool: '<unknown>',
                        args: {},
                        status: action.isError ? 'error' : 'completed',
                        result: action.result,
                        resultStatus: action.isError ? 'error' : 'ok',
                        startedAt: action.timestamp,
                        durationMs: 0,
                    };
                    return {...turn, events: [...turn.events, synthetic]};
                }
                const existing = turn.events[idx] as ToolCallEvent;
                const updated: ToolCallEvent = {
                    ...existing,
                    status: action.isError ? 'error' : 'completed',
                    result: action.result,
                    resultStatus: action.isError ? 'error' : 'ok',
                    durationMs: Math.max(0, action.timestamp - existing.startedAt),
                };
                return {...turn, events: replaceAt(turn.events, idx, updated)};
            });

        case 'permission_request':
            return updateActiveTurn(ensurePlaceholderTurn(state, action.timestamp), (turn) => {
                const idx = turn.events.findIndex(
                    (e) => e.kind === 'tool_call' && e.id === action.toolUseId,
                );
                if (idx >= 0) {
                    const existing = turn.events[idx] as ToolCallEvent;
                    const updated: ToolCallEvent = {
                        ...existing,
                        status: 'pending',
                        decision: undefined,
                    };
                    return {...turn, events: replaceAt(turn.events, idx, updated)};
                }
                const newCall: ToolCallEvent = {
                    kind: 'tool_call',
                    id: action.toolUseId,
                    tool: action.tool,
                    args: action.args,
                    status: 'pending',
                    startedAt: action.timestamp,
                };
                return {...turn, events: [...turn.events, newCall]};
            });

        case 'permission_decision':
            return updateActiveTurn(ensurePlaceholderTurn(state, action.timestamp), (turn) => {
                const idx = turn.events.findIndex(
                    (e) => e.kind === 'tool_call' && e.id === action.toolUseId,
                );
                if (idx < 0) {
                    if (action.decision === 'denied') {
                        // Deny-before-start: synthetic denied entry so the user sees the marker.
                        const synthetic: ToolCallEvent = {
                            kind: 'tool_call',
                            id: action.toolUseId,
                            tool: '<unknown>',
                            args: {},
                            status: 'denied',
                            decision: 'denied',
                            startedAt: action.timestamp,
                            durationMs: 0,
                        };
                        return {...turn, events: [...turn.events, synthetic]};
                    }
                    return turn;
                }
                const existing = turn.events[idx] as ToolCallEvent;
                // 'expired' = the gate was never answered (TTL, or swept at
                // turn end). The call did not run and the user did not refuse
                // — it must not render as a denial, and must not keep
                // spinning as 'pending' either.
                const newStatus =
                    existing.status !== 'pending'
                        ? existing.status
                        : action.decision === 'denied'
                          ? ('denied' as const)
                          : action.decision === 'expired'
                            ? ('expired' as const)
                            : existing.status;
                const updated: ToolCallEvent = {
                    ...existing,
                    decision: action.decision,
                    status: newStatus,
                };
                return {...turn, events: replaceAt(turn.events, idx, updated)};
            });

        default: {
            // Exhaustiveness check.
            const _exhaustive: never = action;
            void _exhaustive;
            return state;
        }
    }
}

/**
 * Reconcile a cold browser reattach with the agent's authoritative in-flight
 * turn. Incremental message persistence means REST history can already contain
 * the first half of that turn, keyed by its first message UUID and marked
 * historical/done. Cursor replay then continues after the browser's cached
 * event id, often into a synthetic `recovered:` turn because `turn.started`
 * was seen before the refresh.
 *
 * The welcome frame supplies the missing join key (`turn_id`). Prefer the
 * historical turn with that logical number as the visual anchor, fold any live
 * or recovered suffix into it, and promote the result to the real live id. One
 * logical turn therefore remains one streaming bubble; it cannot acquire a
 * mid-turn history divider or a premature read-aloud control.
 */
function reattachTurn(
    state: ConversationState,
    turnId: string,
    timestamp: number,
): ConversationState {
    const turnNumber = numericTurnNumber(turnId);
    const historicalIndex =
        turnNumber === undefined
            ? -1
            : state.turns.findIndex(
                  (t) =>
                      t.kind === 'assistant' &&
                      t.historical === true &&
                      t.turnNumber === turnNumber,
              );
    const exactIndex = state.turns.findIndex(
        (t) => t.kind === 'assistant' && t.id === turnId,
    );
    if (exactIndex >= 0) {
        const exact = state.turns[exactIndex] as AssistantTurn;
        // The welcome frame and replay stream use separate transports. If the
        // terminal replay won that race, it is newer than a welcome snapshot
        // that still said "in flight"; never reopen it.
        if (
            state.activeAssistantTurnId !== turnId &&
            (exact.status === 'done' || exact.status === 'error')
        ) {
            return state;
        }
    }
    const activeIndex = state.activeAssistantTurnId
        ? state.turns.findIndex(
              (t) =>
                  t.kind === 'assistant' &&
                  t.id === state.activeAssistantTurnId &&
                  t.recovered === true,
          )
        : -1;

    // The history prefix is the stable visual anchor. Otherwise reuse the
    // real live turn, then the recovered suffix, before creating an empty turn.
    const anchorIndex =
        historicalIndex >= 0
            ? historicalIndex
            : exactIndex >= 0
              ? exactIndex
              : activeIndex;
    if (anchorIndex < 0) {
        const turn: AssistantTurn = {
            kind: 'assistant',
            id: turnId,
            events: [],
            status: 'streaming',
            turnNumber,
            startedAt: timestamp,
        };
        return {
            ...state,
            turns: [...state.turns, turn],
            activeAssistantTurnId: turnId,
        };
    }

    const sourceIndices = [exactIndex, activeIndex]
        .filter((index, position, all) =>
            index >= 0 && index !== anchorIndex && all.indexOf(index) === position,
        )
        .sort((a, b) => a - b);
    const anchor = state.turns[anchorIndex] as AssistantTurn;
    let events = anchor.events;
    let model = anchor.model;
    let startedAt = anchor.startedAt;
    for (const index of sourceIndices) {
        const source = state.turns[index] as AssistantTurn;
        events = mergeReattachedEvents(events, source.events);
        model = source.model ?? model;
        startedAt = Math.min(startedAt, source.startedAt);
    }

    const reconciled: AssistantTurn = {
        ...anchor,
        id: turnId,
        events,
        status: 'streaming',
        turnNumber: turnNumber ?? anchor.turnNumber,
        model,
        startedAt,
        finishedAt: undefined,
        recovered: undefined,
    };
    const removed = new Set(sourceIndices);
    return {
        ...state,
        turns: state.turns
            .map((turn, index) => (index === anchorIndex ? reconciled : turn))
            .filter((_turn, index) => !removed.has(index)),
        activeAssistantTurnId: turnId,
    };
}

function numericTurnNumber(turnId: string): number | undefined {
    if (!turnId.trim()) return undefined;
    const value = Number(turnId);
    return Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

/** Merge id-bearing replay events without duplicating an already-rendered
 * historical tool/thought/compaction. Text frames are deltas with no message
 * id, so they remain ordered append-only. */
function mergeReattachedEvents(base: TurnEvent[], suffix: TurnEvent[]): TurnEvent[] {
    const merged = [...base];
    for (const event of suffix) {
        const existingIndex = merged.findIndex((candidate) => {
            if (candidate.kind !== event.kind) return false;
            if (event.kind === 'thought') {
                return !!event.messageId &&
                    candidate.kind === 'thought' &&
                    candidate.messageId === event.messageId;
            }
            if (event.kind === 'text') return false;
            return candidate.id === event.id;
        });
        if (existingIndex < 0) {
            merged.push(event);
            continue;
        }
        const existing = merged[existingIndex];
        if (existing.kind === 'thought' && event.kind === 'thought') {
            merged[existingIndex] = existing.content.includes(event.content)
                ? existing
                : {...event, content: existing.content + event.content};
        } else {
            merged[existingIndex] = {...existing, ...event} as TurnEvent;
        }
    }
    return merged;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function closeOpenEvents(events: TurnEvent[], timestamp: number): TurnEvent[] {
    return events.map((e) => {
        if (e.kind === 'thought' && e.status === 'streaming') {
            const closed: ThoughtEvent = {
                ...e,
                status: 'done',
                durationMs: Math.max(0, timestamp - e.startedAt),
            };
            return closed;
        }
        if (e.kind === 'text' && e.status === 'streaming') {
            const closed: TextEvent = {...e, status: 'done'};
            return closed;
        }
        return e;
    });
}

/**
 * Open a synthetic placeholder assistant turn when streaming events arrive
 * without an `activeAssistantTurnId` — typically because `connect()` reset
 * state on a mid-turn reconnect and the SSE replay cursor is past the
 * `turn.started` event (so the reducer never saw it).
 *
 * The placeholder absorbs subsequent token / thinking / tool events so the
 * UI surfaces partial state instead of silently losing the turn. When the
 * real `turn.completed` (or `turn.interrupted`) finally arrives, the
 * placeholder is promoted to the real turn id and closed — see the
 * `turn_completed` case above. See
 * knowledge-base/knowledge/issues/persistent_chat_lost_assistant_turn_on_mid_turn_reload.md
 * §Approach 2.
 */
function ensurePlaceholderTurn(
    state: ConversationState,
    timestamp: number,
): ConversationState {
    if (state.activeAssistantTurnId) return state;
    const placeholderId = `recovered:${timestamp}`;
    const placeholder: AssistantTurn = {
        kind: 'assistant',
        id: placeholderId,
        events: [],
        status: 'streaming',
        startedAt: timestamp,
        recovered: true,
    };
    return {
        ...state,
        turns: [...state.turns, placeholder],
        activeAssistantTurnId: placeholderId,
    };
}

/**
 * Append a reasoning delta, with id-keyed dedupe.
 *
 * A `thinking` frame may carry the id of the AI message it belongs to. The
 * same frame can arrive twice across the history/replay seam: history paints
 * the completed turn (thought bubble keyed by the row id), then the SSE replay
 * cursor — saved a few events behind — re-emits the trailing reasoning frame.
 * Because the turn is no longer active, that replayed frame would otherwise
 * land in a synthetic `recovered:` bubble via `ensurePlaceholderTurn`, showing
 * the same reasoning twice. So: if this message's reasoning already lives in
 * some *other* turn, drop the frame. The active turn is exempt, so live deltas
 * for the in-flight message keep appending. Frames without a messageId (older
 * rows, interleaved Anthropic/Responses thinking) fall back to adjacency-only
 * behaviour, exactly as before. See
 * knowledge-base/knowledge/issues/persistent_chat_reasoning_after_answer_and_replay_duplication.md
 */
function appendThought(
    state: ConversationState,
    content: string,
    timestamp: number,
    messageId?: string,
): ConversationState {
    if (messageId) {
        const activeId = state.activeAssistantTurnId;
        const seenElsewhere = state.turns.some(
            (t) =>
                t.kind === 'assistant' &&
                t.id !== activeId &&
                t.events.some((e) => e.kind === 'thought' && e.messageId === messageId),
        );
        if (seenElsewhere) return state;
    }
    const seeded = ensurePlaceholderTurn(state, timestamp);
    return updateActiveTurn(seeded, (turn) => {
        const last = turn.events[turn.events.length - 1];
        if (last && last.kind === 'thought' && last.status === 'streaming') {
            const lastThought = last as ThoughtEvent;
            // Merge into the open thought only when it's the same message (or
            // neither side is keyed). A different messageId starts a fresh
            // bubble even if adjacent.
            const sameMessage =
                !messageId ||
                lastThought.messageId === undefined ||
                lastThought.messageId === messageId;
            if (sameMessage) {
                const merged: ThoughtEvent = {
                    ...lastThought,
                    content: lastThought.content + content,
                    messageId: lastThought.messageId ?? messageId,
                };
                return {...turn, events: replaceAt(turn.events, turn.events.length - 1, merged)};
            }
        }
        const closed = closeOpenEvents(turn.events, timestamp);
        const blockIndex = closed.filter((e) => e.kind === 'thought' || e.kind === 'text').length;
        const id = `${turn.id}.b${blockIndex}`;
        const newEvent: ThoughtEvent = {
            kind: 'thought',
            id,
            messageId,
            content,
            status: 'streaming',
            startedAt: timestamp,
        };
        return {...turn, events: [...closed, newEvent]};
    });
}

/**
 * Drop the active turn's in-progress reasoning bubble for a message id.
 *
 * The agent's empty-response retry (a gpt-5.x turn that streamed reasoning then
 * emitted no answer) sends `thinking.reset` before re-streaming the retry's
 * reasoning, so the dead-end reasoning is REPLACED rather than appended under.
 * We remove only *streaming* thoughts matching `messageId` from the active turn:
 * a `done` thought from an earlier interleaved block survives, and with no
 * active turn (e.g. SSE replay after history already painted the clean persisted
 * row) `updateActiveTurn` no-ops — so this is idempotent and replay-safe. Unlike
 * `appendThought` it never seeds a placeholder turn; reset is purely subtractive.
 */
function resetThought(state: ConversationState, messageId?: string): ConversationState {
    return updateActiveTurn(state, (turn) => ({
        ...turn,
        events: turn.events.filter(
            (e) =>
                !(
                    e.kind === 'thought' &&
                    e.status === 'streaming' &&
                    (!messageId || e.messageId === messageId)
                ),
        ),
    }));
}

function appendDelta(
    state: ConversationState,
    eventKind: 'text' | 'thought',
    content: string,
    timestamp: number,
): ConversationState {
    const seeded = ensurePlaceholderTurn(state, timestamp);
    return updateActiveTurn(seeded, (turn) => {
        const last = turn.events[turn.events.length - 1];
        if (last && last.kind === eventKind) {
            const lastTyped = last as TextEvent | ThoughtEvent;
            if (lastTyped.status === 'streaming') {
                const merged: TurnEvent =
                    eventKind === 'text'
                        ? {...(last as TextEvent), content: (last as TextEvent).content + content}
                        : {...(last as ThoughtEvent), content: (last as ThoughtEvent).content + content};
                return {...turn, events: replaceAt(turn.events, turn.events.length - 1, merged)};
            }
        }
        const closed = closeOpenEvents(turn.events, timestamp);
        const blockIndex = closed.filter((e) => e.kind === 'thought' || e.kind === 'text').length;
        const id = `${turn.id}.b${blockIndex}`;
        const newEvent: TurnEvent =
            eventKind === 'text'
                ? {kind: 'text', id, content, status: 'streaming', startedAt: timestamp}
                : {kind: 'thought', id, content, status: 'streaming', startedAt: timestamp};
        return {...turn, events: [...closed, newEvent]};
    });
}

function updateActiveTurn(
    state: ConversationState,
    updater: (t: AssistantTurn) => AssistantTurn,
): ConversationState {
    if (!state.activeAssistantTurnId) return state;
    return {
        ...state,
        turns: state.turns.map((t) => {
            if (t.kind !== 'assistant' || t.id !== state.activeAssistantTurnId) return t;
            return updater(t);
        }),
    };
}

function replaceAt<T>(arr: T[], idx: number, value: T): T[] {
    const next = arr.slice();
    next[idx] = value;
    return next;
}
