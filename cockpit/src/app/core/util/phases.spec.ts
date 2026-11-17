import {describe, expect, it} from 'vitest';
import de from '../../../assets/i18n/de-DE.json';
import en from '../../../assets/i18n/en.json';
import {
  PHASES_KEY,
  expertPlansInPhases,
  jobPhases,
  phasesFragment,
  phasesValueKey,
  plansInPhases,
  worksWithoutPhases,
} from './phases';

function lookup(tree: unknown, key: string): unknown {
  return key.split('.').reduce<unknown>((node, part) => (node as Record<string, unknown> | undefined)?.[part], tree);
}

describe('a job\'s phases value', () => {
  it('reads only a boolean; anything else is unknown', () => {
    expect(jobPhases({phases: true})).toBe(true);
    expect(jobPhases({phases: false})).toBe(false);
    expect(jobPhases({phases: null})).toBeNull();
    expect(jobPhases({})).toBeNull();
    expect(jobPhases(null)).toBeNull();
    expect(jobPhases(undefined)).toBeNull();
    expect(jobPhases({phases: 'true'})).toBeNull();
  });

  it('counts an unknown job neither as phased nor as direct', () => {
    expect(plansInPhases({phases: true})).toBe(true);
    expect(plansInPhases({phases: false})).toBe(false);
    expect(plansInPhases({phases: null})).toBe(false);
    expect(worksWithoutPhases({phases: false})).toBe(true);
    expect(worksWithoutPhases({phases: true})).toBe(false);
    expect(worksWithoutPhases({phases: null})).toBe(false);
    expect(worksWithoutPhases({})).toBe(false);
  });

  it('names Yes and No in both languages, and nothing for an unknown value', () => {
    expect(phasesValueKey(true)).toBe('jobs.detail.phasesYes');
    expect(phasesValueKey(false)).toBe('jobs.detail.phasesNo');
    expect(phasesValueKey(null)).toBeNull();
    for (const key of ['jobs.detail.phases', phasesValueKey(true)!, phasesValueKey(false)!]) {
      expect(typeof lookup(en, key), key).toBe('string');
      expect(typeof lookup(de, key), key).toBe('string');
    }
    expect(en.jobs.detail.phases).toBe('Plans in phases');
    expect(de.jobs.detail.phases).toBe('Plant in Phasen');
  });
});

describe('an expert config\'s phases setting', () => {
  it('is the top-level boolean, off when absent', () => {
    expect(PHASES_KEY).toBe('phases');
    expect(expertPlansInPhases({phases: true})).toBe(true);
    expect(expertPlansInPhases({phases: false})).toBe(false);
    expect(expertPlansInPhases({})).toBe(false);
    expect(expertPlansInPhases(null)).toBe(false);
    expect(expertPlansInPhases({llm: {phases: true}})).toBe(false);
  });

  it('reads the words the server reads as on, and nothing else', () => {
    for (const word of ['on', 'true', 'yes', ' On ']) expect(expertPlansInPhases({phases: word}), word).toBe(true);
    for (const word of ['off', 'false', 'no', '', 'maybe']) expect(expertPlansInPhases({phases: word}), word).toBe(false);
    expect(expertPlansInPhases({phases: 1})).toBe(false);
  });

  it('writes true when on, false over a stored key when off, and no key otherwise', () => {
    expect(phasesFragment(true, {})).toEqual({phases: true});
    expect(phasesFragment(true, {phases: false})).toEqual({phases: true});
    expect(phasesFragment(false, {phases: true})).toEqual({phases: false});
    expect(phasesFragment(false, {phases: false})).toEqual({phases: false});
    expect(phasesFragment(false, {autonomy: 'full'})).toEqual({});
  });
});

describe('the phases strings', () => {
  it('name no job mode', () => {
    const keys = [
      'jobs.detail.phases',
      'jobs.phasesBadge',
      'jobs.tooltip.phasesBadge',
      'experts.editor.phases',
      'experts.editor.phasesHint',
      'experts.editor.summary.phases',
      'todoList.empty.directTaskHint',
    ];
    for (const key of keys) {
      for (const lang of [en, de]) {
        const text = lookup(lang, key);
        expect(typeof text, key).toBe('string');
        expect(String(text), key).not.toMatch(/agent mode|worker mode|agent-modus|worker-modus|strateg|taktisch|tactical/i);
      }
    }
  });
});
