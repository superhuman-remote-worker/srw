import {describe, expect, it} from 'vitest';
import {
  buildToolResultIndex,
  isLegacyTodosInput,
  legacyInjectKind,
  resolveToolResultState,
  splitTurn,
} from './chat-history.component';
import {ChatEntry, ChatInput} from '../../core/models/chat.model';

function entry(id: string, inputs: ChatInput[]): ChatEntry {
  return {
    _id: id,
    job_id: 'job-1',
    agent_type: 'universal',
    timestamp: '2026-07-30T10:00:00Z',
    iteration: 1,
    model: 'm',
    inputs,
    response: {content_preview: 'ok', has_tool_calls: false},
  };
}

describe('splitTurn', () => {
  it('splits new-style context descriptors out of the delta', () => {
    const t = splitTurn(
      entry('1', [
        {type: 'tool', tool_call_id: 'call_1', content_preview: 'result', truncated: true},
        {type: 'human', content: 'real turn', content_preview: 'real turn'},
        {
          type: 'context',
          kind: 'knowledge',
          hash: 'd449e62d',
          chars: 8214,
          content_preview: '--- Project Knowledge ---',
          truncated: true,
        },
        {type: 'context', kind: 'todos', hash: 'aa11bb22', content_preview: '<active_tasks>…'},
      ]),
    );
    expect(t.humans.map((h) => h.input.content)).toEqual(['real turn']);
    expect(t.context.map((c) => c.kind)).toEqual(['knowledge', 'todos']);
    // truncated marker (lean page) == the block changed this turn.
    expect(t.context[0].updated).toBe(true);
    expect(t.context[1].updated).toBe(false);
    expect(t.context[0].chars).toBe(8214);
  });

  it('classifies legacy raw-injection rows into the context strip', () => {
    const t = splitTurn(
      entry('1', [
        {
          type: 'human',
          content_preview: '<active_tasks>\nCurrent Tasks — Phase 1 (Strategic)',
        },
        {
          type: 'tool',
          tool_call_id: 'knowledge_inject_d449e62d',
          content_preview: '--- Project Knowledge ---',
        },
        {type: 'human', content_preview: 'a genuine user message'},
      ]),
    );
    expect(t.humans.map((h) => h.input.content_preview)).toEqual([
      'a genuine user message',
    ]);
    expect(t.context.map((c) => c.kind)).toEqual(['todos', 'knowledge']);
    // Legacy rows re-stored the block every turn — no change signal available.
    expect(t.context.every((c) => !c.updated)).toBe(true);
  });
});

describe('legacy classification helpers', () => {
  it('recognizes all synthetic injection prefixes', () => {
    for (const [id, kind] of [
      ['instruction_inject_ab12cd34', 'instruction'],
      ['memory_inject_ab12cd34', 'memory'],
      ['knowledge_inject_ab12cd34', 'knowledge'],
      ['charter_inject_ab12cd34', 'charter'],
      ['citation_feedback_inject_ab12cd34', 'citation_feedback'],
      ['guidance_inject_ab12cd34', 'guidance'],
    ] as const) {
      expect(
        legacyInjectKind({type: 'tool', tool_call_id: id, content_preview: ''}),
      ).toBe(kind);
    }
    expect(
      legacyInjectKind({type: 'tool', tool_call_id: 'call_abc', content_preview: ''}),
    ).toBeNull();
  });

  it('only flags human inputs starting with the todos wrapper', () => {
    expect(
      isLegacyTodosInput({type: 'human', content_preview: '<active_tasks>\nx'}),
    ).toBe(true);
    expect(
      isLegacyTodosInput({type: 'human', content_preview: 'mentions <active_tasks>'}),
    ).toBe(false);
    expect(
      isLegacyTodosInput({type: 'tool', content_preview: '<active_tasks>\nx'}),
    ).toBe(false);
  });
});

describe('buildToolResultIndex', () => {
  it('maps results across the loaded window and skips injections', () => {
    const idx = buildToolResultIndex([
      entry('1', []),
      entry('2', [
        {type: 'tool', tool_call_id: 'call_a', content_preview: 'result a'},
        {type: 'tool', tool_call_id: 'knowledge_inject_x', content_preview: 'kb'},
      ]),
      // Empty-delta turn between call and result (e.g. reminder-only turn).
      entry('3', []),
      entry('4', [{type: 'tool', tool_call_id: 'call_b', content_preview: 'result b'}]),
    ]);
    expect(idx.get('call_a')).toEqual({
      entryId: '2',
      input: {type: 'tool', tool_call_id: 'call_a', content_preview: 'result a'},
    });
    expect(idx.get('call_b')?.entryId).toBe('4');
    expect(idx.has('knowledge_inject_x')).toBe(false);
  });
});

describe('resolveToolResultState', () => {
  // Results live in the FOLLOWING turn's inputs, so only the last loaded turn
  // can still be waiting on data. Everything else was never recorded — jobs
  // predating the archiver delta fix lost every tool result this way, and the
  // old code (keyed on hasMore() alone) mislabeled them all as "arrives later".
  it('marks the last loaded turn as unloaded while more turns exist', () => {
    expect(resolveToolResultState('9', '9', true)).toBe('unloaded');
  });

  it('marks earlier turns missing even when the job has more turns', () => {
    // Turn 9's result would be in turn 10, which is already loaded.
    expect(resolveToolResultState('9', '42', true)).toBe('missing');
  });

  it('marks the final turn of a fully loaded job as missing', () => {
    expect(resolveToolResultState('69', '69', false)).toBe('missing');
  });

  it('marks everything missing when the whole job is loaded', () => {
    // The e239ef27 case: 69 turns, all loaded, zero tool results written.
    expect(resolveToolResultState('9', '69', false)).toBe('missing');
  });

  it('treats an empty window as missing rather than perpetually loading', () => {
    expect(resolveToolResultState('9', null, true)).toBe('missing');
  });
});
