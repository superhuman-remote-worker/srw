import type {JobMode} from '../models/api.model';

export type {JobMode};

/** The config key the server reads a job's mode from. An automation has no
 *  field of its own: its mode travels in its `config_override` under this key. */
export const JOB_MODE_KEY = 'job_mode';

/** The two modes in the order the selects offer them, worker (the default)
 *  first. `labelKey` names a job's mode after creation, `optionKey` is the
 *  select's option, `hintKey` the one-line hint under it. */
export const JOB_MODES: ReadonlyArray<{
  value: JobMode;
  labelKey: string;
  optionKey: string;
  hintKey: string;
}> = [
  {
    value: 'worker',
    labelKey: 'jobs.mode.worker.label',
    optionKey: 'jobs.mode.worker.option',
    hintKey: 'jobs.mode.worker.hint',
  },
  {
    value: 'agent',
    labelKey: 'jobs.mode.agent.label',
    optionKey: 'jobs.mode.agent.option',
    hintKey: 'jobs.mode.agent.hint',
  },
];

/** Autonomy levels that stop at phase boundaries. A job in agent mode has no
 *  phases, so it stops once, at its end: these then work like `review`. */
export const PHASE_STOP_AUTONOMY_LEVELS: readonly string[] = ['partial', 'guided', 'dependent'];

/** A mode from any value: only `agent` is agent mode. Missing, empty and
 *  unknown values read as worker mode, the default. */
export function jobModeOf(value: unknown): JobMode {
  return value === 'agent' ? 'agent' : 'worker';
}

/** The mode a job row or detail carries; older rows without one are worker jobs. */
export function isAgentModeJob(job: {job_mode?: unknown} | null | undefined): boolean {
  return jobModeOf(job?.job_mode) === 'agent';
}

/** The i18n key that names a mode ("Worker mode" / "Agent mode"). */
export function jobModeLabelKey(mode: JobMode): string {
  return mode === 'agent' ? 'jobs.mode.agent.label' : 'jobs.mode.worker.label';
}

/** The mode a stored `config_override` names. */
export function jobModeFromOverride(override: unknown): JobMode {
  return jobModeOf(overrideRecord(override)[JOB_MODE_KEY]);
}

/**
 * A copy of `override` with the mode written in: agent mode sets the key,
 * worker mode (the default) removes it. Every other key is kept as it is.
 */
export function withJobMode(override: unknown, mode: JobMode): Record<string, unknown> {
  const next = {...overrideRecord(override)};
  if (mode === 'agent') next[JOB_MODE_KEY] = 'agent';
  else delete next[JOB_MODE_KEY];
  return next;
}

/** A `config_override` as a plain object. JSONB can arrive as JSON text;
 *  anything that is not an object reads as empty. */
export function overrideRecord(value: unknown): Record<string, unknown> {
  let parsed = value;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      return {};
    }
  }
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>)
    : {};
}
