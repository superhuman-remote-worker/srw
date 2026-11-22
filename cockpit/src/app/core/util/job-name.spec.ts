import {describe, expect, it} from 'vitest';
import {jobDisplayName, jobMayStillBeNamed, jobNameSuggestion} from './job-name';

describe('jobDisplayName', () => {
  it('shows the task text for a job nobody named', () => {
    expect(jobDisplayName({description: 'Summarise the contract', title: null})).toBe('Summarise the contract');
    expect(jobDisplayName({description: 'Summarise the contract'})).toBe('Summarise the contract');
  });

  it('shows the name once the job has one', () => {
    expect(jobDisplayName({description: 'Summarise the contract', title: 'Lease review'})).toBe('Lease review');
  });

  it('treats a blank title as no title', () => {
    expect(jobDisplayName({description: 'Summarise the contract', title: '   '})).toBe('Summarise the contract');
  });

  it('is empty, not "undefined", for a missing job', () => {
    expect(jobDisplayName(null)).toBe('');
    expect(jobDisplayName({})).toBe('');
  });
});

describe('jobNameSuggestion', () => {
  it('starts a named job from its name', () => {
    expect(jobNameSuggestion({description: 'Summarise the contract', title: 'Lease review'})).toBe('Lease review');
  });

  it('starts an unnamed job from the first line of its task text', () => {
    expect(jobNameSuggestion({description: '\n  Summarise   the contract \nThen list the risks.'})).toBe(
      'Summarise the contract',
    );
  });

  it('cuts a long first line at a word boundary', () => {
    const description =
      'Read every pull request merged this week and write a summary for the release notes with links';
    const suggestion = jobNameSuggestion({description});

    expect(suggestion.length).toBeLessThanOrEqual(80);
    expect(description.startsWith(suggestion)).toBe(true);
    expect(description[suggestion.length]).toBe(' ');
  });

  it('cuts a long unbroken line at the limit', () => {
    expect(jobNameSuggestion({description: 'x'.repeat(300)})).toBe('x'.repeat(80));
  });

  it('is empty when there is nothing to start from', () => {
    expect(jobNameSuggestion({description: '  \n '})).toBe('');
    expect(jobNameSuggestion(null)).toBe('');
  });
});

describe('jobMayStillBeNamed', () => {
  const NOW = Date.parse('2026-11-22T10:00:30Z');
  const fresh = {title: null, parent_job_id: null, created_at: '2026-11-22T10:00:20Z'};

  it('is true for an unnamed job created seconds ago', () => {
    expect(jobMayStillBeNamed(fresh, NOW)).toBe(true);
  });

  it('is false once the job has a name', () => {
    expect(jobMayStillBeNamed({...fresh, title: 'Release notes'}, NOW)).toBe(false);
  });

  it('is false for a subjob: the server names root jobs only', () => {
    expect(jobMayStillBeNamed({...fresh, parent_job_id: 'job-1'}, NOW)).toBe(false);
  });

  it('is false after the naming window: a name that has not come will not come', () => {
    expect(jobMayStillBeNamed({...fresh, created_at: '2026-11-22T09:59:29Z'}, NOW)).toBe(false);
    expect(jobMayStillBeNamed({...fresh, created_at: '2026-11-22T09:59:31Z'}, NOW)).toBe(true);
  });

  it('is false without a usable creation time', () => {
    expect(jobMayStillBeNamed({...fresh, created_at: null}, NOW)).toBe(false);
    expect(jobMayStillBeNamed({...fresh, created_at: 'not a date'}, NOW)).toBe(false);
  });

  it('tolerates a server clock a little ahead of the browser', () => {
    expect(jobMayStillBeNamed({...fresh, created_at: '2026-11-22T10:00:33Z'}, NOW)).toBe(true);
    expect(jobMayStillBeNamed({...fresh, created_at: '2026-11-22T10:05:00Z'}, NOW)).toBe(false);
  });
});
