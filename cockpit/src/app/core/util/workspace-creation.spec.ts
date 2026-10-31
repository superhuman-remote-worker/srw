import {describe, expect, it} from 'vitest';
import type {WorkspaceCreationView} from '../models/api.model';
import {workspaceCreationMessageKey, workspaceCreationMessageParams} from './workspace-creation';
import en from '../../../assets/i18n/en.json';
import de from '../../../assets/i18n/de-DE.json';

function view(stage: WorkspaceCreationView['stage'], state: WorkspaceCreationView['state'], reason_code: WorkspaceCreationView['reason_code']): WorkspaceCreationView {
  return {stage, state, reason_code, readiness_deadline_at: null, exit_code: null};
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
    [view('readiness', 'attention', 'pull_deadline'), 'finish pulling in time'],
    [view('readiness', 'attention', 'readiness_deadline'), 'become ready in time'],
    [view('readiness', 'attention', 'readiness_deadline'), 'The cluster may also have stopped it'],
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

  it('names the exit code of an exited Job container in both languages', () => {
    const creation = {...view('readiness', 'attention', 'container_exited'), exit_code: 3};
    expect(workspaceCreationMessageKey(creation)).toBe('workspaceCreation.containerExited');
    expect(workspaceCreationMessageParams(creation)).toEqual({exitCode: 3});
    expect(en.workspaceCreation.containerExited).toContain('{{exitCode}}');
    expect(de.workspaceCreation.containerExited).toContain('{{exitCode}}');
  });

  it('keeps exit code 0', () => {
    const creation = {...view('readiness', 'attention', 'container_exited'), exit_code: 0};
    expect(workspaceCreationMessageKey(creation)).toBe('workspaceCreation.containerExited');
    expect(workspaceCreationMessageParams(creation)).toEqual({exitCode: 0});
  });

  it('falls back to the neutral copy without an integer exit code', () => {
    for (const exit_code of [null, undefined, 3.5]) {
      const creation = {...view('readiness', 'attention', 'container_exited'), exit_code} as WorkspaceCreationView;
      expect(workspaceCreationMessageKey(creation)).toBe('workspaceCreation.unknown');
      expect(workspaceCreationMessageParams(creation)).toEqual({});
    }
  });

  it('passes no params for any other reason', () => {
    expect(workspaceCreationMessageParams(view('readiness', 'attention', 'readiness_deadline'))).toEqual({});
  });

  it('pins the approved copy verbatim', () => {
    expect(en.workspaceCreation.containerExited).toBe(
      "Workspace needs attention: its container exited with code {{exitCode}} before it was ready. A workspace image must keep running SRW's SSH server: build it FROM an SRW base image and don't override its ENTRYPOINT or USER.");
    expect(de.workspaceCreation.containerExited).toBe(
      'Arbeitsbereich benötigt Aufmerksamkeit: Sein Container wurde mit Code {{exitCode}} beendet, bevor er bereit war. Ein Arbeitsbereich-Image muss den SSH-Server von SRW weiter ausführen: Bauen Sie es auf einem SRW-Basis-Image auf (FROM) und überschreiben Sie weder ENTRYPOINT noch USER.');
    expect(en.workspaceCreation.readinessDeadline).toBe(
      "Workspace needs attention: it didn't become ready in time. If it uses your own image, check that the image keeps running SRW's SSH server: build it FROM an SRW base image and don't override its ENTRYPOINT or USER. The cluster may also have stopped it, for example when it ran out of memory: try a template with more memory.");
    expect(de.workspaceCreation.readinessDeadline).toBe(
      'Arbeitsbereich benötigt Aufmerksamkeit: Er wurde nicht rechtzeitig bereit. Wenn er Ihr eigenes Image verwendet, prüfen Sie, ob das Image den SSH-Server von SRW weiter ausführt: Bauen Sie es auf einem SRW-Basis-Image auf (FROM) und überschreiben Sie weder ENTRYPOINT noch USER. Möglicherweise hat ihn auch der Cluster gestoppt, etwa weil ihm der Arbeitsspeicher ausging: Versuchen Sie eine Vorlage mit mehr Arbeitsspeicher.');
  });
});
