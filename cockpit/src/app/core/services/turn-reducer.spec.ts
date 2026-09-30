import {describe, expect, it} from 'vitest';
import {
    AssistantTurn,
    ConversationState,
    EMPTY_CONVERSATION,
    groupEvents,
    isAssistantTurn,
    isToolCall,
    TextEvent,
    ThoughtEvent,
    ToolCallEvent,
    UserTurn,
} from '../models/turn.model';
import {reduce, ReducerAction} from './turn-reducer';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function play(actions: ReducerAction[], initial: ConversationState = EMPTY_CONVERSATION): ConversationState {
    return actions.reduce((s, a) => reduce(s, a), initial);
}

function activeTurn(state: ConversationState): AssistantTurn {
    const t = state.turns.find(isAssistantTurn);
    if (!t) throw new Error('no assistant turn');
    return t;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('turn-reducer — reset / load_history', () => {
    it('reset clears state with optional threadId', () => {
        const state = play([{type: 'reset', threadId: 'abc'}]);
        expect(state).toEqual({threadId: 'abc', turns: [], activeAssistantTurnId: null});
    });

    it('load_history replaces turns and clears active turn', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'load_history',
                threadId: 'thr',
                turns: [{kind: 'user', id: 'u1', content: 'hi', timestamp: 500}],
            },
        ]);
        expect(state.turns).toHaveLength(1);
        expect(state.activeAssistantTurnId).toBeNull();
    });
});

describe('turn-reducer — user_message / system_message', () => {
    it('appends a UserTurn', () => {
        const state = play([
            {type: 'user_message', id: 'u1', content: 'hello', timestamp: 100},
        ]);
        expect(state.turns).toEqual([
            {kind: 'user', id: 'u1', content: 'hello', timestamp: 100, attachments: undefined},
        ]);
    });

    it('appends a SystemTurn', () => {
        const state = play([
            {type: 'system_message', id: 's1', content: 'Session ended.', timestamp: 200},
        ]);
        expect(state.turns).toEqual([
            {kind: 'system', id: 's1', content: 'Session ended.', timestamp: 200},
        ]);
    });
});

describe('user_message attachments before upload', () => {
    it('keeps distinct ids for attachments that have no server path yet', () => {
        const state = reduce(EMPTY_CONVERSATION, {
            type: 'user_message',
            id: 'user-1',
            content: 'here',
            attachments: [
                {id: 'p1', name: 'a.pdf', size: 1, mimeType: 'application/pdf'},
                {id: 'p2', name: 'b.pdf', size: 2, mimeType: 'application/pdf'},
            ],
            timestamp: 1,
        });

        const turn = state.turns[state.turns.length - 1];
        expect(turn.kind).toBe('user');
        const ids = (turn as UserTurn).attachments?.map((a) => a.id);
        expect(ids).toEqual(['p1', 'p2']);
        expect(new Set(ids).size).toBe(2);
    });
});

describe('turn-reducer — update_attachments', () => {
    it('patches the chips of an already-rendered bubble in place', () => {
        const state = play([
            {
                type: 'user_message',
                id: 'user-1',
                content: 'here',
                attachments: [{id: 'p1', name: 'a.pdf', size: 1, mimeType: 'application/pdf'}],
                timestamp: 1,
            },
            {type: 'user_message', id: 'user-2', content: 'and this', timestamp: 2},
            {
                type: 'update_attachments',
                id: 'user-1',
                // The upload resolved: real path, server-renamed on collision.
                attachments: [
                    {id: 'p1', name: 'a_1.pdf', size: 1, mimeType: 'application/pdf', path: 'uploads/a_1.pdf'},
                ],
            },
        ]);

        // Patched in place: no duplicate bubble, and the message the user
        // queued behind it stays behind it.
        expect(state.turns.map((t) => t.id)).toEqual(['user-1', 'user-2']);
        expect((state.turns[0] as UserTurn).attachments).toEqual([
            {id: 'p1', name: 'a_1.pdf', size: 1, mimeType: 'application/pdf', path: 'uploads/a_1.pdf'},
        ]);
    });

    it('is a no-op when no turn matches the id', () => {
        const state = play([
            {type: 'user_message', id: 'u1', content: 'hi', timestamp: 1},
            {type: 'update_attachments', id: 'gone', attachments: []},
        ]);
        expect(state.turns).toHaveLength(1);
        expect((state.turns[0] as UserTurn).attachments).toBeUndefined();
    });
});

describe('turn-reducer — remove_turn', () => {
    it('removes the turn with the matching id and leaves the others', () => {
        const state = play([
            {type: 'user_message', id: 'u1', content: 'first', timestamp: 100},
            {type: 'user_message', id: 'u2', content: 'second', timestamp: 200},
            {type: 'remove_turn', id: 'u1'},
        ]);
        expect(state.turns).toHaveLength(1);
        expect(state.turns[0].id).toBe('u2');
    });

    it('is a no-op when no turn matches the id', () => {
        const state = play([
            {type: 'user_message', id: 'u1', content: 'hi', timestamp: 100},
            {type: 'remove_turn', id: 'nope'},
        ]);
        expect(state.turns).toHaveLength(1);
        expect(state.turns[0].id).toBe('u1');
    });

    it('does not disturb the active assistant turn', () => {
        const state = play([
            {type: 'user_message', id: 'u1', content: 'hi', timestamp: 100},
            {type: 'turn_started', turnId: 't1', startedAt: 200},
            {type: 'remove_turn', id: 'u1'},
        ]);
        expect(state.turns.find((t) => t.id === 'u1')).toBeUndefined();
        expect(state.activeAssistantTurnId).toBe('t1');
    });
});

describe('turn-reducer — turn lifecycle', () => {
    it('turn_started opens an empty assistant turn and marks it active', () => {
        const state = play([{type: 'turn_started', turnId: 't1', startedAt: 1000, model: 'opus'}]);
        const turn = activeTurn(state);
        expect(turn.status).toBe('streaming');
        expect(turn.events).toEqual([]);
        expect(turn.model).toBe('opus');
        expect(state.activeAssistantTurnId).toBe('t1');
    });

    it('replayed turn_started rebuilds the live turn from the re-delivered frames', () => {
        // A stream that re-anchors before the turn began (stale/horizon-lost
        // cursor) replays turn.started AND every frame after it. Keeping the
        // old events would double the text ("hihi") and stack a second copy
        // of the thoughts/tool calls behind the answer.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'token', content: 'hi', timestamp: 1100},
            {type: 'turn_started', turnId: 't1', startedAt: 1000}, // replay
            {type: 'token', content: 'hi', timestamp: 1100}, // re-delivered
        ]);
        expect(state.turns.filter(isAssistantTurn)).toHaveLength(1);
        const turn = activeTurn(state);
        expect(turn.status).toBe('streaming');
        expect(turn.events).toHaveLength(1);
        expect((turn.events[0] as TextEvent).content).toBe('hi');
        expect(state.activeAssistantTurnId).toBe('t1');
    });

    it('reattach_turn joins a historical prefix and recovered suffix into one live turn', () => {
        const historical: AssistantTurn = {
            kind: 'assistant',
            id: 'history-message-uuid',
            turnNumber: 7,
            historical: true,
            status: 'done',
            startedAt: 500,
            finishedAt: 700,
            events: [
                {
                    kind: 'text',
                    id: 'history-message-uuid.b0',
                    content: 'I will update the draft.',
                    status: 'done',
                    startedAt: 500,
                },
            ],
        };
        let state = reduce(EMPTY_CONVERSATION, {
            type: 'load_history',
            threadId: 'thread-1',
            turns: [
                {kind: 'user', id: 'u7', content: 'Please update it', timestamp: 400},
                historical,
            ],
        });
        // Cursor replay resumes after turn.started, so the first new token
        // initially lands in a recovered suffix.
        state = reduce(state, {type: 'token', content: 'Done.', timestamp: 800});
        expect(state.turns.filter(isAssistantTurn)).toHaveLength(2);

        state = reduce(state, {type: 'reattach_turn', turnId: '7', timestamp: 900});

        const assistants = state.turns.filter(isAssistantTurn);
        expect(assistants).toHaveLength(1);
        expect(assistants[0]).toMatchObject({
            id: '7',
            turnNumber: 7,
            historical: true,
            status: 'streaming',
        });
        expect(assistants[0].finishedAt).toBeUndefined();
        expect(assistants[0].events.map((event) => event.kind)).toEqual(['text', 'text']);
        expect((assistants[0].events[1] as TextEvent).content).toBe('Done.');
        expect(state.activeAssistantTurnId).toBe('7');

        state = reduce(state, {type: 'turn_completed', turnId: '7', finishedAt: 1000});
        expect(state.turns.filter(isAssistantTurn)).toHaveLength(1);
        expect((state.turns[1] as AssistantTurn).status).toBe('done');
        expect(state.activeAssistantTurnId).toBeNull();
    });

    it('reattach_turn reopens a same-thread turn interrupted by transport teardown', () => {
        const interrupted: AssistantTurn = {
            kind: 'assistant',
            id: '12',
            turnNumber: 12,
            events: [],
            status: 'interrupted',
            startedAt: 100,
            finishedAt: 200,
        };
        const state = reduce(
            {threadId: 'thread-1', turns: [interrupted], activeAssistantTurnId: null},
            {type: 'reattach_turn', turnId: '12', timestamp: 300},
        );
        expect(state.turns).toHaveLength(1);
        expect((state.turns[0] as AssistantTurn).status).toBe('streaming');
        expect((state.turns[0] as AssistantTurn).finishedAt).toBeUndefined();
        expect(state.activeAssistantTurnId).toBe('12');
    });

    it('reattach_turn does not reopen a turn already closed by replay', () => {
        const state = play([
            {type: 'turn_started', turnId: '8', startedAt: 100},
            {type: 'token', content: 'finished', timestamp: 150},
            {type: 'turn_completed', turnId: '8', finishedAt: 200},
            // A stale cross-transport welcome snapshot arrives last.
            {type: 'reattach_turn', turnId: '8', timestamp: 250},
        ]);
        const turn = state.turns[0] as AssistantTurn;
        expect(turn.status).toBe('done');
        expect(turn.finishedAt).toBe(200);
        expect(state.activeAssistantTurnId).toBeNull();
    });

    it('turn_started rebuilds a matching historical prefix during full cold replay', () => {
        const historical: AssistantTurn = {
            kind: 'assistant',
            id: 'history-message-uuid',
            turnNumber: 4,
            historical: true,
            events: [
                {
                    kind: 'text',
                    id: 'history-message-uuid.b0',
                    content: 'persisted prefix',
                    status: 'done',
                    startedAt: 100,
                },
            ],
            status: 'done',
            startedAt: 100,
            finishedAt: 200,
        };
        const state = play(
            [
                {type: 'turn_started', turnId: '4', startedAt: 100},
                {type: 'token', content: 'persisted prefix', timestamp: 110},
            ],
            {threadId: 'thread-1', turns: [historical], activeAssistantTurnId: null},
        );
        const assistants = state.turns.filter(isAssistantTurn);
        expect(assistants).toHaveLength(1);
        expect(assistants[0]).toMatchObject({
            id: '4',
            turnNumber: 4,
            historical: true,
            status: 'streaming',
        });
        expect((assistants[0].events[0] as TextEvent).content).toBe('persisted prefix');
    });

    it('turn_started keeps the run a recovery replaced, also when the stream re-anchors', () => {
        // History: the replaced run's call with its settled result, then the
        // successor's answer. The replay starts at the successor's turn.started.
        const call: ToolCallEvent = {
            kind: 'tool_call',
            id: 'd1',
            tool: 'delegate_agent',
            args: {},
            status: 'completed',
            result: 'report of d1',
            startedAt: 100,
        };
        const historical: AssistantTurn = {
            kind: 'assistant',
            id: 'history-message-uuid',
            turnNumber: 4,
            historical: true,
            priorRunEvents: 1,
            events: [
                call,
                {kind: 'text', id: 'history-message-uuid.b1', content: 'answer', status: 'done', startedAt: 300},
            ],
            status: 'done',
            startedAt: 100,
            finishedAt: 300,
        };
        const replay: ReducerAction[] = [
            {type: 'turn_started', turnId: '4', startedAt: 1000},
            {type: 'token', content: 'answer', timestamp: 1010},
        ];
        const once = play(replay, {threadId: 'thread-1', turns: [historical], activeAssistantTurnId: null});
        const twice = play(replay, once);
        for (const state of [once, twice]) {
            const assistants = state.turns.filter(isAssistantTurn);
            expect(assistants).toHaveLength(1);
            expect(assistants[0].events.map((e) => e.kind)).toEqual(['tool_call', 'text']);
            expect(assistants[0].events[0]).toBe(call);
            expect((assistants[0].events[1] as TextEvent).content).toBe('answer');
        }
    });

    it('turn_completed flips status to done and clears activeAssistantTurnId', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'turn_completed', turnId: 't1', finishedAt: 2000},
        ]);
        const turn = activeTurn(state);
        expect(turn.status).toBe('done');
        expect(turn.finishedAt).toBe(2000);
        expect(state.activeAssistantTurnId).toBeNull();
    });

    it('turn_interrupted flips status to interrupted', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'turn_interrupted', turnId: 't1', finishedAt: 1500},
        ]);
        const turn = activeTurn(state);
        expect(turn.status).toBe('interrupted');
    });

    it('turn_completed mid-stream closes open text/thinking events', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'pondering', timestamp: 1100},
            {type: 'turn_completed', turnId: 't1', finishedAt: 2000},
        ]);
        const turn = activeTurn(state);
        expect(turn.events).toHaveLength(1);
        expect((turn.events[0] as ThoughtEvent).status).toBe('done');
        expect((turn.events[0] as ThoughtEvent).durationMs).toBe(900);
    });

    it('a new turn_started while a prior turn is still streaming closes the prior turn', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'token', content: 'partial', timestamp: 1100},
            {type: 'turn_started', turnId: 't2', startedAt: 2000},
        ]);
        const t1 = state.turns.find((t) => isAssistantTurn(t) && t.id === 't1') as AssistantTurn;
        expect(t1.status).toBe('done');
        expect(t1.finishedAt).toBe(2000);
        expect(state.activeAssistantTurnId).toBe('t2');
    });
});

describe('turn-reducer — text/thinking deltas', () => {
    it('consecutive tokens append to the same TextEvent', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'token', content: 'Hello ', timestamp: 1100},
            {type: 'token', content: 'world', timestamp: 1200},
        ]);
        const turn = activeTurn(state);
        expect(turn.events).toHaveLength(1);
        expect((turn.events[0] as TextEvent).content).toBe('Hello world');
        expect((turn.events[0] as TextEvent).status).toBe('streaming');
    });

    it('consecutive thinking deltas append to the same ThoughtEvent', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'I should ', timestamp: 1100},
            {type: 'thinking', content: 'plan first', timestamp: 1200},
        ]);
        const turn = activeTurn(state);
        expect(turn.events).toHaveLength(1);
        expect((turn.events[0] as ThoughtEvent).content).toBe('I should plan first');
    });

    it('token after thinking opens a new TextEvent and closes the ThoughtEvent', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'planning', timestamp: 1100},
            {type: 'token', content: 'Sure!', timestamp: 1200},
        ]);
        const turn = activeTurn(state);
        expect(turn.events).toHaveLength(2);
        expect(turn.events[0].kind).toBe('thought');
        expect((turn.events[0] as ThoughtEvent).status).toBe('done');
        expect(turn.events[1].kind).toBe('text');
    });

    it('thinking after a tool_call opens a new ThoughtEvent (interleaved-thinking pattern)', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'plan', timestamp: 1100},
            {
                type: 'tool_started',
                toolUseId: 'tc1',
                tool: 'read_file',
                args: {},
                timestamp: 1200,
            },
            {type: 'tool_completed', toolUseId: 'tc1', result: 'ok', timestamp: 1300},
            {type: 'thinking', content: 'reflect', timestamp: 1400},
        ]);
        const turn = activeTurn(state);
        const thoughts = turn.events.filter((e) => e.kind === 'thought') as ThoughtEvent[];
        expect(thoughts).toHaveLength(2);
        expect(thoughts[0].content).toBe('plan');
        expect(thoughts[1].content).toBe('reflect');
    });

    it('deltas without an active turn open a recovered placeholder turn (Approach 2)', () => {
        // Defense-in-depth from
        // knowledge-base/knowledge/issues/persistent_chat_lost_assistant_turn_on_mid_turn_reload.md:
        // when streaming events arrive with activeAssistantTurnId === null
        // (e.g. SSE replay cursor is past turn.started after a mid-turn
        // reconnect), the reducer now synthesises a placeholder turn so the
        // partial state is visible instead of silently lost.
        const state = play([{type: 'token', content: 'orphan', timestamp: 1000}]);
        expect(state.turns).toHaveLength(1);
        const turn = state.turns[0];
        expect(turn.kind).toBe('assistant');
        if (turn.kind !== 'assistant') throw new Error('expected assistant turn');
        expect(turn.recovered).toBe(true);
        expect(turn.id).toBe('recovered:1000');
        expect(turn.status).toBe('streaming');
        expect(turn.events).toHaveLength(1);
        expect(turn.events[0].kind).toBe('text');
        expect(state.activeAssistantTurnId).toBe('recovered:1000');
    });

    it('turn_completed promotes a recovered placeholder to the real turn id', () => {
        // When the real turn.completed event finally arrives after a
        // placeholder was synthesised by orphan deltas, the placeholder is
        // renamed to the real turn id and closed — so the bubble doesn't
        // hang in "streaming" forever.
        const state = play([
            {type: 'token', content: 'orphan content', timestamp: 1000},
            {type: 'turn_completed', turnId: 'real-turn-id', finishedAt: 2000},
        ]);
        expect(state.turns).toHaveLength(1);
        const turn = state.turns[0];
        if (turn.kind !== 'assistant') throw new Error('expected assistant turn');
        expect(turn.id).toBe('real-turn-id');
        expect(turn.status).toBe('done');
        expect(turn.finishedAt).toBe(2000);
        expect(state.activeAssistantTurnId).toBeNull();
    });

    it('block ids are stable: ${turnId}.b<index>', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'a', timestamp: 1100},
            {type: 'token', content: 'b', timestamp: 1200},
            {type: 'thinking', content: 'c', timestamp: 1300},
        ]);
        const turn = activeTurn(state);
        expect(turn.events.map((e) => e.id)).toEqual(['t1.b0', 't1.b1', 't1.b2']);
    });
});

describe('turn-reducer — reasoning frame dedupe (message-id keyed)', () => {
    // knowledge-base/knowledge/issues/persistent_chat_reasoning_after_answer_and_replay_duplication.md
    // For gemma-style models, the reasoning frame is journaled after the whole
    // token run; an SSE replay cursor landing in that gap re-emits just the
    // thinking frame after the turn closed, and ensurePlaceholderTurn would
    // otherwise materialise it as a duplicate `recovered:` bubble.

    function historicalTurn(messageId: string): AssistantTurn {
        return {
            kind: 'assistant',
            id: 'hist-turn',
            events: [
                {
                    kind: 'thought',
                    id: 'hist-turn.b0',
                    messageId,
                    content: 'because X',
                    status: 'done',
                    startedAt: 500,
                },
                {kind: 'text', id: 'hist-turn.b1', content: 'answer', status: 'done', startedAt: 600},
            ],
            status: 'done',
            startedAt: 500,
            finishedAt: 600,
            historical: true,
        };
    }

    it('drops a replayed thinking frame whose message already rendered in another turn', () => {
        const loaded = reduce(EMPTY_CONVERSATION, {
            type: 'load_history',
            threadId: 'th',
            turns: [historicalTurn('msg-x')],
        });
        // Replay re-emits the trailing reasoning frame; activeAssistantTurnId
        // is null because the turn already closed.
        const after = reduce(loaded, {
            type: 'thinking',
            content: 'because X',
            messageId: 'msg-x',
            timestamp: 1000,
        });
        expect(after.turns).toHaveLength(1);
        expect(after.turns[0].id).toBe('hist-turn');
        expect(after.activeAssistantTurnId).toBeNull();
        const thoughts = (after.turns[0] as AssistantTurn).events.filter((e) => e.kind === 'thought');
        expect(thoughts).toHaveLength(1);
    });

    it('a full live turn then a trailing replayed reasoning frame yields one bubble', () => {
        let state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'reason', messageId: 'msg-1', timestamp: 1100},
            {type: 'token', content: 'answer', timestamp: 1200},
            {type: 'turn_completed', turnId: 't1', finishedAt: 1300},
        ]);
        state = reduce(state, {
            type: 'thinking',
            content: 'reason',
            messageId: 'msg-1',
            timestamp: 1400,
        });
        expect(state.turns).toHaveLength(1);
        const thoughts = (state.turns[0] as AssistantTurn).events.filter((e) => e.kind === 'thought');
        expect(thoughts).toHaveLength(1);
        expect(state.activeAssistantTurnId).toBeNull();
    });

    it('live deltas for the active message keep appending (active turn exempt)', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'a', messageId: 'msg-1', timestamp: 1100},
            {type: 'thinking', content: 'b', messageId: 'msg-1', timestamp: 1200},
        ]);
        const thoughts = activeTurn(state).events.filter((e) => e.kind === 'thought') as ThoughtEvent[];
        expect(thoughts).toHaveLength(1);
        expect(thoughts[0].content).toBe('ab');
        expect(thoughts[0].messageId).toBe('msg-1');
    });

    it('adjacent thinking frames with different message ids form separate bubbles', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'first', messageId: 'msg-1', timestamp: 1100},
            {type: 'thinking', content: 'second', messageId: 'msg-2', timestamp: 1200},
        ]);
        const thoughts = activeTurn(state).events.filter((e) => e.kind === 'thought') as ThoughtEvent[];
        expect(thoughts).toHaveLength(2);
        expect(thoughts.map((t) => t.messageId)).toEqual(['msg-1', 'msg-2']);
    });

    it('an orphan thinking frame with no message id still recovers (back-compat)', () => {
        const state = play([{type: 'thinking', content: 'orphan', timestamp: 1000}]);
        expect(state.turns).toHaveLength(1);
        expect(state.turns[0].id).toBe('recovered:1000');
    });

    it('an orphan thinking frame for an unrendered message still recovers', () => {
        const state = play([
            {type: 'thinking', content: 'new', messageId: 'msg-new', timestamp: 1000},
        ]);
        expect(state.turns).toHaveLength(1);
        const turn = state.turns[0] as AssistantTurn;
        expect(turn.recovered).toBe(true);
        expect((turn.events[0] as ThoughtEvent).messageId).toBe('msg-new');
    });
});

describe('turn-reducer — thinking_reset (live replace)', () => {
    it('drops the active streaming reasoning bubble for a message id', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'dead ', timestamp: 1100, messageId: 'a1'},
            {type: 'thinking', content: 'end', timestamp: 1150, messageId: 'a1'},
            {type: 'thinking_reset', messageId: 'a1', timestamp: 1200},
        ]);
        const turn = activeTurn(state);
        expect(turn.events.filter((e) => e.kind === 'thought')).toHaveLength(0);
    });

    it('lets the retry re-stream a fresh bubble after a reset', () => {
        // The empty-response replace: dead-end reasoning is cleared, then the
        // retry streams its own reasoning + answer under the same message id.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'dead end', timestamp: 1100, messageId: 'a1'},
            {type: 'thinking_reset', messageId: 'a1', timestamp: 1200},
            {type: 'thinking', content: 'fresh reasoning', timestamp: 1300, messageId: 'a1'},
            {type: 'token', content: 'recovered', timestamp: 1400},
        ]);
        const turn = activeTurn(state);
        const thoughts = turn.events.filter((e) => e.kind === 'thought') as ThoughtEvent[];
        expect(thoughts).toHaveLength(1);
        expect(thoughts[0].content).toBe('fresh reasoning'); // dead-end gone
        const texts = turn.events.filter((e) => e.kind === 'text') as TextEvent[];
        expect(texts[0].content).toBe('recovered');
    });

    it('is a no-op when there is no active turn (replay-safe)', () => {
        const state = play([{type: 'thinking_reset', messageId: 'a1', timestamp: 1000}]);
        expect(state).toEqual(EMPTY_CONVERSATION);
    });

    it('leaves a done thought and a non-matching streaming thought intact', () => {
        // a1 closes to done when a2 opens; resetting a1 matches neither the
        // (done) a1 nor the (streaming, different-id) a2 — nothing is removed.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'first', timestamp: 1100, messageId: 'a1'},
            {type: 'thinking', content: 'second', timestamp: 1200, messageId: 'a2'},
            {type: 'thinking_reset', messageId: 'a1', timestamp: 1300},
        ]);
        const turn = activeTurn(state);
        const thoughts = turn.events.filter((e) => e.kind === 'thought') as ThoughtEvent[];
        expect(thoughts).toHaveLength(2);
        expect(thoughts.map((t) => t.content)).toEqual(['first', 'second']);
    });

    it('an UNKEYED reset removes every streaming thought of the active turn', () => {
        // The agent sends an unkeyed reset when attempt-1's dead-end reasoning
        // was broadcast as UNKEYED in-stream block frames (Responses/codex) —
        // only an unkeyed reset can clear those bubbles. Done thoughts stay.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'finished lead-up', timestamp: 1050, messageId: 'a0'},
            {type: 'token', content: 'text closes a0 to done', timestamp: 1075},
            {type: 'thinking', content: 'unkeyed dead-end', timestamp: 1100},
            {type: 'thinking', content: 'keyed dead-end', timestamp: 1150, messageId: 'a1'},
            {type: 'thinking_reset', timestamp: 1200},
        ]);
        const turn = activeTurn(state);
        const thoughts = turn.events.filter((e) => e.kind === 'thought') as ThoughtEvent[];
        expect(thoughts).toHaveLength(1);
        expect(thoughts[0].content).toBe('finished lead-up');
        expect(thoughts[0].status).toBe('done');
    });
});

describe('turn-reducer — tool calls', () => {
    it('tool_started pushes a ToolCallEvent in running state', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'tool_started',
                toolUseId: 'tc1',
                tool: 'read_file',
                args: {path: 'x'},
                category: 'workspace',
                timestamp: 1100,
            },
        ]);
        const turn = activeTurn(state);
        const tc = turn.events[0] as ToolCallEvent;
        expect(tc.kind).toBe('tool_call');
        expect(tc.tool).toBe('read_file');
        expect(tc.status).toBe('running');
        expect(tc.category).toBe('workspace');
    });

    it('tool_completed updates by id, sets durationMs', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'tool_started',
                toolUseId: 'tc1',
                tool: 'read_file',
                args: {},
                timestamp: 1100,
            },
            {type: 'tool_completed', toolUseId: 'tc1', result: 'data', timestamp: 1500},
        ]);
        const turn = activeTurn(state);
        const tc = turn.events[0] as ToolCallEvent;
        expect(tc.status).toBe('completed');
        expect(tc.result).toBe('data');
        expect(tc.durationMs).toBe(400);
        expect(tc.resultStatus).toBe('ok');
    });

    it('tool_completed with isError flips to error status', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'tool_started',
                toolUseId: 'tc1',
                tool: 'run_command',
                args: {},
                timestamp: 1100,
            },
            {
                type: 'tool_completed',
                toolUseId: 'tc1',
                result: 'boom',
                isError: true,
                timestamp: 1200,
            },
        ]);
        const tc = activeTurn(state).events[0] as ToolCallEvent;
        expect(tc.status).toBe('error');
        expect(tc.resultStatus).toBe('error');
    });

    it('orphan tool_completed (no prior tool_started) creates a synthetic completed entry', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'tool_completed', toolUseId: 'tc-orphan', result: 'r', timestamp: 1100},
        ]);
        const turn = activeTurn(state);
        const tc = turn.events.find(isToolCall);
        expect(tc).toBeDefined();
        expect(tc!.status).toBe('completed');
        expect(tc!.id).toBe('tc-orphan');
    });

    it('tool_started closes any open thinking event before pushing the tool call', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {type: 'thinking', content: 'let me check', timestamp: 1100},
            {
                type: 'tool_started',
                toolUseId: 'tc1',
                tool: 'read_file',
                args: {},
                timestamp: 1200,
            },
        ]);
        const turn = activeTurn(state);
        expect(turn.events).toHaveLength(2);
        expect((turn.events[0] as ThoughtEvent).status).toBe('done');
        expect(turn.events[1].kind).toBe('tool_call');
    });
});

describe('turn-reducer — message key (which response issued a call)', () => {
    // Tool frames carry no message id. The key decides whether two
    // delegate_agent calls are one fan-out (one batch card) or two responses.
    const started = (id: string, timestamp: number): ReducerAction => ({
        type: 'tool_started', toolUseId: id, tool: 'delegate_agent', args: {}, timestamp,
    });
    const completed = (id: string, timestamp: number): ReducerAction => ({
        type: 'tool_completed', toolUseId: id, result: 'report', timestamp,
    });
    const keys = (state: ConversationState) =>
        activeTurn(state).events.filter(isToolCall).map((e) => e.messageKey);

    it('gives the calls announced together one key', () => {
        // The loop announces every call of a response before any result.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 0},
            started('d1', 10), started('d2', 11), started('d3', 12),
        ]);
        expect(new Set(keys(state)).size).toBe(1);
    });

    it('starts a new key once the previous calls have returned', () => {
        // The model is asked again only after every result is in, so a call
        // after a completed one belongs to the next response.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 0},
            started('d1', 10), started('d2', 11),
            completed('d1', 50), completed('d2', 51),
            started('d3', 60), started('d4', 61),
        ]);
        const [k1, k2, k3, k4] = keys(state);
        expect(k1).toBe(k2);
        expect(k3).toBe(k4);
        expect(k3).not.toBe(k1);
        expect(groupEvents(activeTurn(state).events).map((g) => g.kind))
            .toEqual(['delegate_batch', 'delegate_batch']);
    });

    it('starts a new key after a thought', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 0},
            started('d1', 10),
            {type: 'thinking', content: 'next', timestamp: 20},
            started('d2', 30),
        ]);
        const [k1, k2] = keys(state);
        expect(k1).not.toBe(k2);
    });

    it('keeps the key of a supervised batch through approval and start', () => {
        // permission.request_batch announces all calls as pending first; the
        // key is set there and survives the promotion to running.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 0},
            {type: 'permission_request', toolUseId: 'd1', tool: 'delegate_agent', args: {}, timestamp: 10},
            {type: 'permission_request', toolUseId: 'd2', tool: 'delegate_agent', args: {}, timestamp: 11},
            {type: 'permission_decision', toolUseId: 'd1', decision: 'approved', timestamp: 20},
            {type: 'permission_decision', toolUseId: 'd2', decision: 'denied', timestamp: 21},
            started('d1', 30),
        ]);
        const [k1, k2] = keys(state);
        expect(k1).toBeTruthy();
        expect(k1).toBe(k2);
    });

    it('keeps the history row id when a cold reattach replays part of a fan-out', () => {
        const historical: AssistantTurn = {
            kind: 'assistant',
            id: 'row-1',
            turnNumber: 7,
            historical: true,
            status: 'done',
            startedAt: 500,
            events: ['d1', 'd2'].map((id) => ({
                kind: 'tool_call' as const,
                id,
                tool: 'delegate_agent',
                args: {},
                status: 'completed' as const,
                startedAt: 500,
                messageKey: 'row-1',
            })),
        };
        let state = reduce(EMPTY_CONVERSATION, {type: 'load_history', threadId: 'th', turns: [historical]});
        // Replay resumes after d1's frame: d2's start lands in a recovered
        // suffix with a synthetic key of its own.
        state = reduce(state, started('d2', 800));
        state = reduce(state, {type: 'reattach_turn', turnId: '7', timestamp: 900});

        const turn = state.turns.filter(isAssistantTurn)[0];
        expect(turn.events.filter(isToolCall).map((e) => e.messageKey)).toEqual(['row-1', 'row-1']);
        expect(groupEvents(turn.events).map((g) => g.kind)).toEqual(['delegate_batch']);
    });
});

describe('turn-reducer — permissions', () => {
    it('permission_request before tool_started creates a pending ToolCallEvent', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'permission_request',
                toolUseId: 'tc1',
                tool: 'run_command',
                args: {cmd: 'rm'},
                timestamp: 1100,
            },
        ]);
        const tc = activeTurn(state).events[0] as ToolCallEvent;
        expect(tc.status).toBe('pending');
        expect(tc.tool).toBe('run_command');
    });

    it('permission_request followed by tool_started promotes the entry to running', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'permission_request',
                toolUseId: 'tc1',
                tool: 'run_command',
                args: {cmd: 'ls'},
                timestamp: 1100,
            },
            {
                type: 'tool_started',
                toolUseId: 'tc1',
                tool: 'run_command',
                args: {cmd: 'ls'},
                timestamp: 1200,
            },
        ]);
        const turn = activeTurn(state);
        expect(turn.events).toHaveLength(1);
        expect((turn.events[0] as ToolCallEvent).status).toBe('running');
    });

    it('permission_decision (denied) on a pending call flips status to denied', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'permission_request',
                toolUseId: 'tc1',
                tool: 'x',
                args: {},
                timestamp: 1100,
            },
            {
                type: 'permission_decision',
                toolUseId: 'tc1',
                decision: 'denied',
                timestamp: 1200,
            },
        ]);
        const tc = activeTurn(state).events[0] as ToolCallEvent;
        expect(tc.status).toBe('denied');
        expect(tc.decision).toBe('denied');
    });

    it('permission_decision without a prior request creates a synthetic denied entry', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'permission_decision',
                toolUseId: 'tc-ghost',
                decision: 'denied',
                timestamp: 1100,
            },
        ]);
        const tc = activeTurn(state).events[0] as ToolCallEvent;
        expect(tc).toBeDefined();
        expect(tc.status).toBe('denied');
        expect(tc.id).toBe('tc-ghost');
    });

    it('permission_decision (expired) marks the call unanswered, NOT denied', () => {
        // A gate that timed out or was swept at turn end is not a refusal.
        // Painting it "denied" is the fabricated-denial bug, in the UI:
        // knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'permission_request',
                toolUseId: 'tc1',
                tool: 'web_search',
                args: {},
                timestamp: 1100,
            },
            {
                type: 'permission_decision',
                toolUseId: 'tc1',
                decision: 'expired',
                timestamp: 1200,
            },
        ]);
        const tc = activeTurn(state).events[0] as ToolCallEvent;
        expect(tc.status).toBe('expired');
        expect(tc.status).not.toBe('denied');
        expect(tc.decision).toBe('expired');
    });

    it('permission_decision (expired) without a prior request fabricates nothing', () => {
        // The denied path synthesises a marker so a deny-before-start is
        // visible. An expired gate must NOT invent a row the user never saw.
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'permission_decision',
                toolUseId: 'tc-ghost',
                decision: 'expired',
                timestamp: 1100,
            },
        ]);
        expect(activeTurn(state).events).toHaveLength(0);
    });

    it('permission_decision (approved) records the decision but does not change a running call', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1000},
            {
                type: 'tool_started',
                toolUseId: 'tc1',
                tool: 'x',
                args: {},
                timestamp: 1100,
            },
            {
                type: 'permission_decision',
                toolUseId: 'tc1',
                decision: 'approved',
                timestamp: 1150,
            },
        ]);
        const tc = activeTurn(state).events[0] as ToolCallEvent;
        expect(tc.status).toBe('running');
        expect(tc.decision).toBe('approved');
    });
});

describe('turn-reducer — replay idempotency', () => {
    const sequence: ReducerAction[] = [
        {type: 'reset', threadId: 'thr'},
        {type: 'user_message', id: 'u1', content: 'go', timestamp: 100},
        {type: 'turn_started', turnId: 't1', startedAt: 200},
        {type: 'thinking', content: 'planning', timestamp: 250},
        {type: 'token', content: 'Sure ', timestamp: 300},
        {type: 'token', content: 'thing.', timestamp: 350},
        {
            type: 'tool_started',
            toolUseId: 'tc1',
            tool: 'read_file',
            args: {p: 'a'},
            timestamp: 400,
        },
        {type: 'tool_completed', toolUseId: 'tc1', result: 'r', timestamp: 500},
        {type: 'thinking', content: 'reflect', timestamp: 550},
        {type: 'turn_completed', turnId: 't1', finishedAt: 600},
    ];

    it('replaying the full sequence twice yields the same final state', () => {
        const once = play(sequence);
        const twice = play([...sequence, ...sequence]);
        expect(twice).toEqual(once);
    });

    it('produces the expected event structure', () => {
        const state = play(sequence);
        const turn = state.turns.find(isAssistantTurn)!;
        const kinds = turn.events.map((e) => e.kind);
        // thought, text, tool_call, thought — boundary detection at work
        expect(kinds).toEqual(['thought', 'text', 'tool_call', 'thought']);
        expect(turn.status).toBe('done');
    });
});

describe('turn-reducer — add_compaction', () => {
    it('appends a compaction banner turn between turns (no turn open)', () => {
        const state = play([
            {type: 'user_message', id: 'u1', content: 'hi', timestamp: 100},
            {type: 'add_compaction', id: 'compaction-3', summary: 'We did X.', timestamp: 200},
        ]);
        expect(state.turns).toHaveLength(2);
        const c = state.turns[1];
        expect(c.kind).toBe('compaction');
        expect(c.id).toBe('compaction-3');
        if (c.kind === 'compaction') expect(c.summary).toBe('We did X.');
    });

    it('is idempotent by id (replay replaces, no duplicate)', () => {
        const state = play([
            {type: 'add_compaction', id: 'compaction-3', summary: 'first', timestamp: 200},
            {type: 'add_compaction', id: 'compaction-3', summary: 'updated', timestamp: 200},
        ]);
        expect(state.turns).toHaveLength(1);
        const c = state.turns[0];
        expect(c.kind).toBe('compaction');
        if (c.kind === 'compaction') expect(c.summary).toBe('updated');
    });

    // Auto-compaction fires mid-turn. The banner must land where it fired, not
    // trail the whole turn — post-compaction work renders inside the open turn,
    // so a top-level divider gets stranded at the foot of the transcript and
    // then jumps into place on reload (when historyToTurns inlines it).
    it('lands inline at its true position when a turn is open', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1},
            {type: 'tool_started', toolUseId: 'a', tool: 'edit_citation', args: {}, timestamp: 2},
            {type: 'tool_completed', toolUseId: 'a', result: 'ok', timestamp: 3},
            {type: 'add_compaction', id: 'compaction-2', summary: 'We did X.', timestamp: 4},
            {type: 'thinking', content: 'more', timestamp: 5},
            {type: 'tool_started', toolUseId: 'b', tool: 'edit_citation', args: {}, timestamp: 6},
        ]);
        // One turn only — no trailing top-level divider.
        expect(state.turns).toHaveLength(1);
        const turn = state.turns.find(isAssistantTurn)!;
        expect(turn.events.map((e) => e.kind)).toEqual([
            'tool_call',
            'compaction',
            'thought',
            'tool_call',
        ]);
        const marker = turn.events[1];
        expect(marker.id).toBe('compaction-2');
        if (marker.kind === 'compaction') expect(marker.summary).toBe('We did X.');
    });

    it('replays an inline marker in place, even after its turn closed', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1},
            {type: 'add_compaction', id: 'compaction-2', summary: 'first', timestamp: 2},
            {type: 'token', content: 'answer', timestamp: 3},
            {type: 'turn_completed', turnId: 't1', finishedAt: 4},
            // Replay after the turn closed: must not spawn a second divider.
            {type: 'add_compaction', id: 'compaction-2', summary: 'updated', timestamp: 2},
        ]);
        expect(state.turns).toHaveLength(1);
        const turn = state.turns.find(isAssistantTurn)!;
        expect(turn.events.map((e) => e.kind)).toEqual(['compaction', 'text']);
        const marker = turn.events[0];
        if (marker.kind === 'compaction') expect(marker.summary).toBe('updated');
    });

    it('does not merge reasoning across a compaction boundary', () => {
        const state = play([
            {type: 'turn_started', turnId: 't1', startedAt: 1},
            {type: 'thinking', content: 'before', timestamp: 2},
            {type: 'add_compaction', id: 'compaction-2', summary: 'We did X.', timestamp: 3},
            {type: 'thinking', content: 'after', timestamp: 4},
        ]);
        const turn = state.turns.find(isAssistantTurn)!;
        expect(turn.events.map((e) => e.kind)).toEqual(['thought', 'compaction', 'thought']);
    });
});
