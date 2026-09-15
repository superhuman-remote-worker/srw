import {describe, it, expect} from 'vitest';
import {
  defaultSelectableReasoning,
  getReasoningOptions,
  getSelectableReasoningOptions,
  reasoningOptionsForModel,
} from './reasoning-options';

describe('getReasoningOptions (capability-driven)', () => {
  it('offers GLM-5.3 Low/High/Max with Max as default and no disable option', () => {
    const cap = {method: 'effort_enum' as const, default: 'max', options: ['low', 'high', 'max']};
    expect(getReasoningOptions(cap)).toEqual([
      {value: null, label: 'Default'},
      {value: 'low', label: 'Low'},
      {value: 'high', label: 'High'},
      {value: 'max', label: 'Max'},
    ]);
    expect(defaultSelectableReasoning(cap)).toBe('max');
  });

  it('returns Default-only for method=none', () => {
    const opts = getReasoningOptions({method: 'none', default: null, options: []});
    expect(opts).toEqual([{value: null, label: 'Default'}]);
  });

  it('returns Default-only for a missing/null capability', () => {
    expect(getReasoningOptions(null)).toEqual([{value: null, label: 'Default'}]);
    expect(getReasoningOptions(undefined)).toEqual([{value: null, label: 'Default'}]);
  });

  it('maps effort_enum options to labelled entries after Default', () => {
    const opts = getReasoningOptions({
      method: 'effort_enum',
      default: 'high',
      options: ['low', 'medium', 'high'],
    });
    expect(opts).toEqual([
      {value: null, label: 'Default'},
      {value: 'low', label: 'Low'},
      {value: 'medium', label: 'Medium'},
      {value: 'high', label: 'High'},
    ]);
  });

  it('renders binary_toggle as On/Off (gemma)', () => {
    const opts = getReasoningOptions({
      method: 'binary_toggle',
      default: 'on',
      options: ['on', 'off'],
    });
    expect(opts).toEqual([
      {value: null, label: 'Default'},
      {value: 'on', label: 'On'},
      {value: 'off', label: 'Off'},
    ]);
  });

  it('labels the OpenRouter superset incl. xhigh', () => {
    const opts = getReasoningOptions({
      method: 'effort_enum',
      default: 'high',
      options: ['none', 'minimal', 'low', 'medium', 'high', 'xhigh'],
    });
    expect(opts.map((o) => o.label)).toEqual([
      'Default',
      'None',
      'Minimal',
      'Low',
      'Medium',
      'High',
      'X-High',
    ]);
  });

  it('shows a non-selectable "Always on" for always_on models', () => {
    const opts = getReasoningOptions({method: 'always_on', default: 'on', options: []});
    expect(opts).toEqual([{value: null, label: 'Always on'}]);
  });
});

describe('reasoningOptionsForModel', () => {
  const byModel = {
    'gemma-4-moe': {method: 'binary_toggle', default: 'on', options: ['on', 'off']},
    'gpt-5.4': {method: 'effort_enum', default: 'high', options: ['low', 'medium', 'high']},
  };

  it('looks up the capability by model id', () => {
    expect(reasoningOptionsForModel('gemma-4-moe', byModel).map((o) => o.label)).toEqual([
      'Default',
      'On',
      'Off',
    ]);
  });

  it('falls back to Default-only for unknown / null models', () => {
    expect(reasoningOptionsForModel('mystery-model', byModel)).toEqual([
      {value: null, label: 'Default'},
    ]);
    expect(reasoningOptionsForModel(null, byModel)).toEqual([
      {value: null, label: 'Default'},
    ]);
  });
});

describe('getSelectableReasoningOptions (no Default sentinel)', () => {
  it('returns the concrete options for a toggle family', () => {
    expect(
      getSelectableReasoningOptions({method: 'binary_toggle', default: 'on', options: ['on', 'off']}),
    ).toEqual([
      {value: 'on', label: 'On'},
      {value: 'off', label: 'Off'},
    ]);
  });

  it('is empty when there is nothing to choose', () => {
    expect(getSelectableReasoningOptions(null)).toEqual([]);
    expect(getSelectableReasoningOptions({method: 'none', default: null, options: []})).toEqual([]);
    expect(getSelectableReasoningOptions({method: 'always_on', default: 'on', options: []})).toEqual([]);
    expect(getSelectableReasoningOptions({method: 'effort_enum', default: 'high', options: []})).toEqual([]);
  });
});

describe('defaultSelectableReasoning', () => {
  it('exposes mandatory Muse reasoning from minimal through max, defaulting to medium', () => {
    const cap = {
      method: 'effort_enum',
      default: 'medium',
      options: ['minimal', 'low', 'medium', 'high', 'xhigh', 'max'],
    };
    expect(getSelectableReasoningOptions(cap).map((option) => option.label)).toEqual([
      'Minimal', 'Low', 'Medium', 'High', 'X-High', 'Max',
    ]);
    expect(defaultSelectableReasoning(cap)).toBe('medium');
  });

  it('resolves to the family default when selectable', () => {
    expect(
      defaultSelectableReasoning({method: 'binary_toggle', default: 'on', options: ['on', 'off']}),
    ).toBe('on');
    expect(
      defaultSelectableReasoning({method: 'effort_enum', default: 'high', options: ['low', 'medium', 'high']}),
    ).toBe('high');
  });

  it('falls back to the first option when the default is out of set', () => {
    expect(
      defaultSelectableReasoning({method: 'effort_enum', default: 'turbo', options: ['low', 'medium', 'high']}),
    ).toBe('low');
    expect(
      defaultSelectableReasoning({method: 'effort_enum', default: null, options: ['low', 'high']}),
    ).toBe('low');
  });

  it('is null when nothing is selectable', () => {
    expect(defaultSelectableReasoning(null)).toBeNull();
    expect(defaultSelectableReasoning({method: 'none', default: null, options: []})).toBeNull();
  });
});
