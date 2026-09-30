import {describe, expect, it} from 'vitest';
import type {WorkspaceCreationView} from '../models/api.model';
import {workspaceCreationMessageKey} from './workspace-creation';
import en from '../../../assets/i18n/en.json';
import de from '../../../assets/i18n/de-DE.json';

function view(stage: WorkspaceCreationView['stage'], state: WorkspaceCreationView['state'], reason_code: WorkspaceCreationView['reason_code']): WorkspaceCreationView {
  return {stage, state, reason_code, readiness_deadline_at: null};
}

describe('typed workspace creation copy', () => {
  it.each([
    [view('scheduling', 'observing', 'observation_pending'), 'Checking workspace scheduling'],
    [view('scheduling', 'waiting_capacity', 'scheduler_unschedulable'), 'Waiting for scheduling'],
    [view('scheduling', 'observing', 'scheduling_other'), 'Checking workspace scheduling'],
    [view('scheduling', 'observing', 'legacy_receipt_held'), 'older workspace creation'],
    [view('readiness', 'starting', 'scheduled'), 'Starting the scheduled workspace'],
    [view('readiness', 'attention', 'invalid_image'), 'image is invalid'],
    [view('readiness', 'attention', 'invalid_configuration'), 'configuration is invalid'],
    [view('readiness', 'attention', 'pull_deadline'), 'image pull exceeded'],
    [view('readiness', 'attention', 'readiness_deadline'), 'readiness exceeded'],
    [view('readiness', 'attention', 'ssh_deadline'), 'SSH authentication grace expired'],
    [view('scheduling', 'waiting_capacity', 'insufficient_capacity'), 'insufficient capacity'],
  ] as const)('renders safe English and German copy for %j', (creation, fragment) => {
    const key = workspaceCreationMessageKey(creation);
    const name = key.split('.')[1];
    expect((en.workspaceCreation as Record<string, string>)[name]).toContain(fragment);
    expect((de.workspaceCreation as Record<string, string>)[name]).toBeTruthy();
  });

  it('uses a neutral fallback for an unknown combination and never reflects diagnostics', () => {
    const creation = {...view('readiness', 'observing', 'scheduler_unschedulable'),
      raw_diagnostic: '0/3 nodes had private taint'} as WorkspaceCreationView;
    const key = workspaceCreationMessageKey(creation);
    expect(key).toBe('workspaceCreation.unknown');
    expect(en.workspaceCreation.unknown).not.toContain('0/3 nodes');
  });
});
