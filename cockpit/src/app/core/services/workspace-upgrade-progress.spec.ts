import { describe, expect, it } from 'vitest';

import {
  advanceUpgradeProgress,
  resumeUpgradeProgress,
  startUpgradeProgress,
  upgradeCardTitle,
  upgradeProgressRows,
  upgradeStepForPhase,
} from './workspace-upgrade-progress';

const rows = (p: Parameters<typeof upgradeProgressRows>[0], now: number) =>
  upgradeProgressRows(p, now).map(({ key, state, elapsedMs }) => ({ key, state, elapsedMs }));

describe('workspace upgrade progress', () => {
  it('a new upgrade counts on its first step and leaves the rest to come', () => {
    const p = startUpgradeProgress('sandbox', null, 1_000);
    expect(rows(p, 1_500)).toEqual([
      { key: 'requested', state: 'active', elapsedMs: 500 },
      { key: 'starting', state: 'todo', elapsedMs: null },
      { key: 'copying', state: 'todo', elapsedMs: null },
    ]);
  });

  it('a step that finished keeps its own time and the next one starts counting', () => {
    let p = startUpgradeProgress('sandbox', null, 1_000);
    p = advanceUpgradeProgress(p, 'starting', 1_300);
    p = advanceUpgradeProgress(p, 'copying', 25_300);
    expect(rows(p, 27_300)).toEqual([
      { key: 'requested', state: 'done', elapsedMs: 300 },
      { key: 'starting', state: 'done', elapsedMs: 24_000 },
      { key: 'copying', state: 'active', elapsedMs: 2_000 },
    ]);
  });

  it('never moves back to an earlier step', () => {
    let p = startUpgradeProgress('vm', null, 0);
    p = advanceUpgradeProgress(p, 'copying', 10_000);
    expect(advanceUpgradeProgress(p, 'starting', 11_000)).toBe(p);
    expect(advanceUpgradeProgress(p, 'copying', 12_000)).toBe(p);
  });

  it('a step it never saw shows as done without a time', () => {
    let p = startUpgradeProgress('sandbox', null, 0);
    p = advanceUpgradeProgress(p, 'copying', 4_000);
    expect(rows(p, 5_000)).toEqual([
      { key: 'requested', state: 'done', elapsedMs: 4_000 },
      { key: 'starting', state: 'done', elapsedMs: null },
      { key: 'copying', state: 'active', elapsedMs: 1_000 },
    ]);
  });

  it('after a reload the current step counts from when the upgrade was requested', () => {
    const started = Date.parse('2026-10-28T09:30:00Z');
    const p = resumeUpgradeProgress(
      { target_tier: 'sandbox', phase: 'copying', started_at: '2026-10-28T09:30:00+00:00' },
      started + 65_000,
    );
    expect(rows(p!, started + 65_000)).toEqual([
      { key: 'requested', state: 'done', elapsedMs: null },
      { key: 'starting', state: 'done', elapsedMs: null },
      { key: 'copying', state: 'active', elapsedMs: 65_000 },
    ]);
  });

  it('a reload without a start time counts from now', () => {
    const p = resumeUpgradeProgress({ target_tier: 'vm', phase: 'provisioning' }, 7_000);
    expect(rows(p!, 9_000)[1]).toEqual({ key: 'starting', state: 'active', elapsedMs: 2_000 });
  });

  it('a failed or missing upgrade resumes nothing', () => {
    expect(resumeUpgradeProgress({ target_tier: 'sandbox', phase: 'failed' }, 0)).toBeNull();
    expect(resumeUpgradeProgress(null, 0)).toBeNull();
  });

  it('maps the server phases to steps', () => {
    expect(upgradeStepForPhase('requested')).toBe('requested');
    expect(upgradeStepForPhase('provisioning')).toBe('starting');
    expect(upgradeStepForPhase('copying')).toBe('copying');
    expect(upgradeStepForPhase('failed')).toBeNull();
  });

  it('names the starting step after the tier', () => {
    const label = (tier: string) =>
      upgradeProgressRows(startUpgradeProgress(tier, null, 0), 0)[1].labelKey;
    expect(label('sandbox')).toBe('chat.workspaceUpgrade.steps.startingContainer');
    expect(label('vm')).toBe('chat.workspaceUpgrade.steps.startingVm');
    expect(label('next')).toBe('chat.workspaceUpgrade.steps.startingWorkspace');
  });

  it('titles the card by template, then by tier', () => {
    expect(upgradeCardTitle(startUpgradeProgress('next', 'vm-big', 0))).toEqual({
      key: 'chat.workspaceUpgrade.card.titleTemplate',
      params: { template: 'vm-big' },
    });
    expect(upgradeCardTitle(startUpgradeProgress('sandbox', null, 0)).key).toBe(
      'chat.workspaceUpgrade.card.titleContainer',
    );
    expect(upgradeCardTitle(startUpgradeProgress('vm', null, 0)).key).toBe(
      'chat.workspaceUpgrade.card.titleVm',
    );
    expect(upgradeCardTitle(startUpgradeProgress('next', null, 0)).key).toBe(
      'chat.workspaceUpgrade.card.title',
    );
  });
});
