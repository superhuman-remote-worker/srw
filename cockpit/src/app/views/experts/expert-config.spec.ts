import {describe, expect, it} from 'vitest';
import {
  assembleExpertConfig,
  liftLegacyTiers,
  MANAGED_CONFIG_KEYS,
  REPLACED_CONFIG_KEYS,
  splitExpertConfig,
} from './expert-config';

describe('MANAGED_CONFIG_KEYS', () => {
  it('covers the dimensions owned by the structured controls', () => {
    for (const k of ['tools', 'llm', 'autonomy', 'workspace', 'limits', 'interactive']) {
      expect(MANAGED_CONFIG_KEYS.has(k)).toBe(true);
    }
    expect(MANAGED_CONFIG_KEYS.has('instruction_files')).toBe(false);
  });

  it('owns phases, which the "Plans in phases" toggle edits', () => {
    expect(MANAGED_CONFIG_KEYS.has('phases')).toBe(true);
    const {managed, rawRemainderText} = splitExpertConfig({phases: true, instruction_files: []});
    expect(managed).toEqual({phases: true});
    expect(JSON.parse(rawRemainderText)).toEqual({instruction_files: []});
    // Kept when the toggle emits nothing, replaced when it writes false,
    // and a hand-written value in the JSON field still wins last.
    expect(assembleExpertConfig({phases: true}, {}, {})).toEqual({phases: true});
    expect(assembleExpertConfig({phases: true}, {phases: false}, {})).toEqual({phases: false});
    expect(assembleExpertConfig({}, {}, {phases: true})).toEqual({phases: true});
  });

  it('owns the subagents roster so it never leaks into the raw-JSON flap', () => {
    expect(MANAGED_CONFIG_KEYS.has('subagents')).toBe(true);
    expect(REPLACED_CONFIG_KEYS).toContain('subagents');
  });
});

describe('splitExpertConfig', () => {
  it('separates managed keys from the raw remainder', () => {
    const {managed, rawRemainderText} = splitExpertConfig({
      tools: {shell: []},
      llm: {model: 'm'},
      instruction_files: [{file: 'x.md'}],
      agent_id: 'scholar',
    });
    expect(managed).toEqual({tools: {shell: []}, llm: {model: 'm'}});
    expect(JSON.parse(rawRemainderText)).toEqual({
      instruction_files: [{file: 'x.md'}],
      agent_id: 'scholar',
    });
  });

  it('keeps a roster out of the remainder', () => {
    const {managed, rawRemainderText} = splitExpertConfig({
      subagents: {default: 'explorer', roster: {explorer: {$ref: 'subagents/explorer'}}},
    });
    expect(managed['subagents']).toBeDefined();
    expect(rawRemainderText).toBe('');
  });

  it('empty remainder yields empty string', () => {
    expect(splitExpertConfig({tools: {}}).rawRemainderText).toBe('');
  });

  it('empty fragment yields empty managed + empty string', () => {
    expect(splitExpertConfig({})).toEqual({managed: {}, rawRemainderText: ''});
  });
});

describe('assembleExpertConfig', () => {
  it('preserves a managed value the control did not re-emit (model survives)', () => {
    // The highest-risk case: model-select / model-group emits no override for a
    // pinned-but-untouched model; managedBase must carry it through.
    const out = assembleExpertConfig({llm: {model: 'X'}}, {}, {});
    expect(out).toEqual({llm: {model: 'X'}});
  });

  it('structured edit beats the stale managed base', () => {
    const out = assembleExpertConfig({autonomy: 'review'}, {autonomy: 'full'}, {});
    expect(out['autonomy']).toBe('full');
  });

  it('preserves an unowned subkey when only a sibling is overridden', () => {
    const out = assembleExpertConfig(
      {workspace: {structure: ['repo/'], backend: 'sandbox'}},
      {workspace: {backend: 'vm'}},
      {},
    );
    expect(out['workspace']).toEqual({structure: ['repo/'], backend: 'vm'});
  });

  it('raw flap wins last (power-user override)', () => {
    const out = assembleExpertConfig({}, {autonomy: 'full'}, {autonomy: 'guided'});
    expect(out['autonomy']).toBe('guided');
  });

  it('merges structured llm reasoning with the model-select model', () => {
    const out = assembleExpertConfig(
      {},
      {llm: {model: 'X', reasoning_level: 'high'}},
      {},
    );
    expect(out['llm']).toEqual({model: 'X', reasoning_level: 'high'});
  });

  it('replaces the subagents block wholesale so a removed roster entry stays removed', () => {
    // A deep-merge would resurrect `b` from the stored fragment.
    const out = assembleExpertConfig(
      {subagents: {default: 'b', llm: {model: 'old'}, roster: {a: {$ref: 'critic'}, b: {$ref: 'scholar'}}}},
      {subagents: {default: 'a', roster: {a: {$ref: 'critic'}}}},
      {},
    );
    expect(out['subagents']).toEqual({default: 'a', roster: {a: {$ref: 'critic'}}});
  });

  it('keeps the stored subagents block when the controls emit none', () => {
    const stored = {subagents: {roster: {a: {$ref: 'critic'}}}};
    expect(assembleExpertConfig(stored, {llm: {model: 'X'}}, {})).toEqual({
      subagents: {roster: {a: {$ref: 'critic'}}},
      llm: {model: 'X'},
    });
  });

  it('does not mutate the source fragment', () => {
    const frag = {tools: {shell: []}};
    assembleExpertConfig(frag, {tools: {shell: ['run_command']}}, {});
    expect(frag).toEqual({tools: {shell: []}});
  });
});

// Client-side mirror of the loader's `normalize_llm_tiers` (U1): a fragment
// authored before the tier collapse must prefill the single-model controls
// correctly and be saved back in the new shape.
describe('liftLegacyTiers', () => {
  it('returns a fragment without legacy tiers by identity', () => {
    const frag = {llm: {model: 'm', temperature: 0.2}, tools: {}};
    expect(liftLegacyTiers(frag)).toBe(frag);
    const none = {tools: {}};
    expect(liftLegacyTiers(none)).toBe(none);
  });

  it('lifts llm.strategic (model AND params) into llm when no explicit model is set', () => {
    const out = liftLegacyTiers({
      llm: {temperature: 0.7, strategic: {model: 'strat', reasoning_level: 'high'}},
    });
    expect(out['llm']).toEqual({temperature: 0.7, model: 'strat', reasoning_level: 'high'});
  });

  it('strategic beats tactical; tactical lifts only without a strategic block', () => {
    expect(liftLegacyTiers({llm: {strategic: {model: 's'}, tactical: {model: 't'}}})['llm']).toEqual({
      model: 's',
    });
    expect(liftLegacyTiers({llm: {tactical: {model: 't'}}})['llm']).toEqual({model: 't'});
  });

  it('prefers the block that carries a model over a params-only one', () => {
    const out = liftLegacyTiers({llm: {strategic: {temperature: 0.1}, tactical: {model: 't'}}});
    expect(out['llm']).toEqual({model: 't'});
  });

  it('layer-local: an explicit llm.model wins and the phase blocks are dropped (July rule)', () => {
    const out = liftLegacyTiers({llm: {model: 'top', strategic: {model: 'strat', temperature: 0.1}}});
    expect(out['llm']).toEqual({model: 'top'});
  });

  it('merged: strategic.model beats the merged dict\'s llm.model (the base placeholder)', () => {
    const out = liftLegacyTiers(
      {llm: {model: 'base-placeholder', strategic: {model: 'strat'}}},
      {merged: true},
    );
    expect(out['llm']).toEqual({model: 'strat'});
  });

  it('moves llm.subagent to subagents.llm unless one is already authored', () => {
    expect(liftLegacyTiers({llm: {model: 'm', subagent: {model: 'reader'}}})).toEqual({
      llm: {model: 'm'},
      subagents: {llm: {model: 'reader'}},
    });
    const authored = liftLegacyTiers({
      llm: {model: 'm', subagent: {model: 'old'}},
      subagents: {llm: {model: 'new'}, roster: {}},
    });
    expect(authored).toEqual({llm: {model: 'm'}, subagents: {llm: {model: 'new'}, roster: {}}});
  });

  it('keeps an existing roster next to the lifted subagent llm', () => {
    const out = liftLegacyTiers({
      llm: {subagent: {model: 'reader'}},
      subagents: {roster: {explorer: {$ref: 'subagents/explorer'}}},
    });
    expect(out['subagents']).toEqual({
      roster: {explorer: {$ref: 'subagents/explorer'}},
      llm: {model: 'reader'},
    });
  });

  it('never lets a null leaf inside a lifted block clear a base key', () => {
    const out = liftLegacyTiers({llm: {base_url: 'http://x', strategic: {model: 's', base_url: null}}});
    expect(out['llm']).toEqual({base_url: 'http://x', model: 's'});
  });

  it('drops empty tier blocks and never mutates the input', () => {
    const frag = {llm: {model: 'm', strategic: {}, tactical: null, subagent: {}}};
    const snapshot = JSON.parse(JSON.stringify(frag));
    const out = liftLegacyTiers(frag);
    expect(out).toEqual({llm: {model: 'm'}});
    expect(frag).toEqual(snapshot);
  });
});
