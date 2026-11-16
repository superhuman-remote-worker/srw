import {describe, expect, it} from 'vitest';
import de from '../../../assets/i18n/de-DE.json';
import en from '../../../assets/i18n/en.json';
import {
  JOB_MODES,
  isAgentModeJob,
  jobModeFromOverride,
  jobModeLabelKey,
  jobModeOf,
  withJobMode,
} from './job-mode';

function lookup(tree: unknown, key: string): unknown {
  return key.split('.').reduce<unknown>((node, part) => (node as Record<string, unknown> | undefined)?.[part], tree);
}

describe('job mode', () => {
  it('reads only "agent" as agent mode; missing and unknown values are worker mode', () => {
    expect(jobModeOf('agent')).toBe('agent');
    expect(jobModeOf('worker')).toBe('worker');
    expect(jobModeOf(undefined)).toBe('worker');
    expect(jobModeOf(null)).toBe('worker');
    expect(jobModeOf('')).toBe('worker');
    expect(jobModeOf('Agent')).toBe('worker');
  });

  it('treats a row without the field as a worker job', () => {
    expect(isAgentModeJob({job_mode: 'agent'})).toBe(true);
    expect(isAgentModeJob({job_mode: 'worker'})).toBe(false);
    expect(isAgentModeJob({})).toBe(false);
    expect(isAgentModeJob(null)).toBe(false);
  });

  it('writes agent mode into a config_override and keeps every other key', () => {
    const stored = {autonomy: 'full', llm: {model: 'm1'}};
    expect(withJobMode(stored, 'agent')).toEqual({autonomy: 'full', llm: {model: 'm1'}, job_mode: 'agent'});
    expect(stored).toEqual({autonomy: 'full', llm: {model: 'm1'}});
  });

  it('removes the key for worker mode, the default', () => {
    expect(withJobMode({job_mode: 'agent', autonomy: 'full'}, 'worker')).toEqual({autonomy: 'full'});
    expect(withJobMode(null, 'worker')).toEqual({});
    expect(withJobMode(undefined, 'agent')).toEqual({job_mode: 'agent'});
  });

  it('reads a stored override, including one that arrived as JSON text', () => {
    expect(jobModeFromOverride({job_mode: 'agent'})).toBe('agent');
    expect(jobModeFromOverride('{"job_mode": "agent", "autonomy": "full"}')).toBe('agent');
    expect(withJobMode('{"autonomy": "full"}', 'agent')).toEqual({autonomy: 'full', job_mode: 'agent'});
    expect(jobModeFromOverride('not json')).toBe('worker');
    expect(jobModeFromOverride(['agent'])).toBe('worker');
    expect(jobModeFromOverride(null)).toBe('worker');
  });

  it('names both modes in both languages', () => {
    for (const mode of JOB_MODES) {
      for (const key of [mode.labelKey, mode.optionKey, mode.hintKey, jobModeLabelKey(mode.value)]) {
        expect(typeof lookup(en, key), key).toBe('string');
        expect(typeof lookup(de, key), key).toBe('string');
      }
    }
    expect(JOB_MODES.map((m) => m.value)).toEqual(['worker', 'agent']);
  });

  it('keeps the new strings free of phase jargon', () => {
    const keys = [
      ...JOB_MODES.flatMap((m) => [m.labelKey, m.optionKey, m.hintKey]),
      'jobs.mode.label',
      'jobs.detail.mode',
      'automations.editor.jobMode',
      'agentSettings.autonomy.agentModeNote',
      'todoList.empty.agentModeHint',
    ];
    for (const key of keys) {
      for (const lang of [en, de]) {
        const text = String(lookup(lang, key));
        expect(text, key).not.toMatch(/strateg|taktisch|tactical/i);
      }
    }
  });
});
