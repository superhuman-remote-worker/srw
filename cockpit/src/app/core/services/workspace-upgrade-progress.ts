/**
 * The steps of an in-place workspace upgrade, as the chat's upgrade card shows
 * them: the same look as the "Starting session" card, with the upgrade's own
 * steps. Pure, so the service can feed it from the 202, the agent's frames and
 * the Session read's `workspace_upgrade` phases alike.
 */

export type UpgradeStep = 'requested' | 'starting' | 'copying';

const STEP_ORDER: readonly UpgradeStep[] = ['requested', 'starting', 'copying'];

export interface UpgradeProgress {
  /** `sandbox`, `vm`, or `next` while the server picks the tier. */
  tier: string;
  /** A named template, shown in the title instead of the tier. */
  template: string | null;
  active: UpgradeStep;
  /** When each step this tab saw became active (epoch ms). */
  starts: Partial<Record<UpgradeStep, number>>;
  /** How long each step this tab saw took (ms). */
  durations: Partial<Record<UpgradeStep, number>>;
}

export interface UpgradeStepRow {
  key: UpgradeStep;
  state: 'done' | 'active' | 'todo';
  elapsedMs: number | null;
  labelKey: string;
}

/** The phase fields of the Session read's `workspace_upgrade`. */
export interface UpgradePhaseView {
  target_tier: string;
  phase: string;
  started_at?: string | null;
}

export function startUpgradeProgress(
  tier: string,
  template: string | null,
  now: number,
): UpgradeProgress {
  return { tier, template, active: 'requested', starts: { requested: now }, durations: {} };
}

/** The step a server phase stands for; null for `failed` or anything unknown. */
export function upgradeStepForPhase(phase: string): UpgradeStep | null {
  if (phase === 'requested') return 'requested';
  if (phase === 'provisioning') return 'starting';
  if (phase === 'copying') return 'copying';
  return null;
}

/** An upgrade found running on load: its current step counts from the
 *  server's `started_at` (the request), or from now without one. */
export function resumeUpgradeProgress(
  view: UpgradePhaseView | null,
  now: number,
): UpgradeProgress | null {
  const step = view ? upgradeStepForPhase(view.phase) : null;
  if (!view || !step) return null;
  const started = view.started_at ? Date.parse(view.started_at) : NaN;
  return {
    tier: view.target_tier,
    template: null,
    active: step,
    starts: { [step]: Number.isFinite(started) ? Math.min(started, now) : now },
    durations: {},
  };
}

/** Move forward to `step`; an earlier or the same step changes nothing. */
export function advanceUpgradeProgress(
  progress: UpgradeProgress,
  step: UpgradeStep,
  now: number,
): UpgradeProgress {
  if (STEP_ORDER.indexOf(step) <= STEP_ORDER.indexOf(progress.active)) return progress;
  const durations = { ...progress.durations };
  const start = progress.starts[progress.active];
  if (start != null) durations[progress.active] = Math.max(0, now - start);
  return {
    ...progress,
    active: step,
    starts: { ...progress.starts, [step]: now },
    durations,
  };
}

export function upgradeProgressRows(progress: UpgradeProgress, now: number): UpgradeStepRow[] {
  const activeIdx = STEP_ORDER.indexOf(progress.active);
  return STEP_ORDER.map((key, idx) => {
    const state = idx < activeIdx ? 'done' : idx === activeIdx ? 'active' : 'todo';
    let elapsedMs: number | null = null;
    if (state === 'done') {
      elapsedMs = progress.durations[key] ?? null;
    } else if (state === 'active' && progress.starts[key] != null) {
      elapsedMs = Math.max(0, now - progress.starts[key]!);
    }
    return { key, state, elapsedMs, labelKey: stepLabelKey(key, progress.tier) };
  });
}

export function upgradeCardTitle(progress: UpgradeProgress): {
  key: string;
  params?: Record<string, string>;
} {
  if (progress.template) {
    return {
      key: 'chat.workspaceUpgrade.card.titleTemplate',
      params: { template: progress.template },
    };
  }
  if (progress.tier === 'sandbox') return { key: 'chat.workspaceUpgrade.card.titleContainer' };
  if (progress.tier === 'vm') return { key: 'chat.workspaceUpgrade.card.titleVm' };
  return { key: 'chat.workspaceUpgrade.card.title' };
}

function stepLabelKey(step: UpgradeStep, tier: string): string {
  if (step !== 'starting') return `chat.workspaceUpgrade.steps.${step}`;
  if (tier === 'sandbox') return 'chat.workspaceUpgrade.steps.startingContainer';
  if (tier === 'vm') return 'chat.workspaceUpgrade.steps.startingVm';
  return 'chat.workspaceUpgrade.steps.startingWorkspace';
}
