import {describe, expect, it} from 'vitest';
import {deepMergeConfig, omitKeys, pickKeys} from './config-merge';

describe('deepMergeConfig', () => {
  it('recurses nested objects', () => {
    expect(deepMergeConfig({a: {x: 1, y: 2}}, {a: {y: 3, z: 4}})).toEqual({
      a: {x: 1, y: 3, z: 4},
    });
  });

  it('override replaces arrays (does not merge)', () => {
    expect(deepMergeConfig({t: [1, 2, 3]}, {t: []})).toEqual({t: []});
  });

  it('override replaces scalars', () => {
    expect(deepMergeConfig({a: 1}, {a: 2})).toEqual({a: 2});
  });

  it('keeps base keys absent from override', () => {
    expect(deepMergeConfig({a: 1, b: 2}, {b: 3})).toEqual({a: 1, b: 3});
  });

  it('does not mutate either input', () => {
    const base = {a: {x: 1}};
    const over = {a: {y: 2}};
    deepMergeConfig(base, over);
    expect(base).toEqual({a: {x: 1}});
    expect(over).toEqual({a: {y: 2}});
  });

  it('skips prototype keys, so neither the result nor Object.prototype changes', () => {
    const over = JSON.parse(
      '{"__proto__": {"polluted": true}, "constructor": {"prototype": {"polluted": true}}, ' +
        '"llm": {"__proto__": {"polluted": true}, "model": "m"}}',
    );
    const merged = deepMergeConfig({llm: {temperature: 1}}, over);
    expect(merged).toEqual({llm: {temperature: 1, model: 'm'}});
    expect(Object.getPrototypeOf(merged)).toBe(Object.prototype);
    expect(Object.getPrototypeOf(merged['llm'])).toBe(Object.prototype);
    expect(({} as Record<string, unknown>)['polluted']).toBeUndefined();
  });
});

describe('pickKeys / omitKeys', () => {
  const src = () => ({tools: {a: 1}, llm: {b: 2}, instruction_files: [1], agent_id: 'x'});

  it('pickKeys keeps only the named top-level keys', () => {
    expect(pickKeys(src(), ['tools', 'llm'])).toEqual({tools: {a: 1}, llm: {b: 2}});
  });

  it('omitKeys drops the named top-level keys (accepts a Set)', () => {
    expect(omitKeys(src(), new Set(['tools', 'llm']))).toEqual({
      instruction_files: [1],
      agent_id: 'x',
    });
  });

  it('do not mutate the source', () => {
    const s = src();
    pickKeys(s, ['tools']);
    omitKeys(s, ['tools']);
    expect(Object.keys(s)).toEqual(['tools', 'llm', 'instruction_files', 'agent_id']);
  });
});
