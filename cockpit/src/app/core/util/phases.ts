/**
 * Whether a job plans in phases (`shared.job_phases` on the server).
 *
 * It is a setting of the Expert: the boolean `phases` at the top level of the
 * expert's config, off when absent. Off, a job gets its task, works on it with
 * its tools and ends when it is done. On, it plans, works through to-dos in
 * phases and reviews its result. A job created through the API may override
 * it; the Cockpit's forms offer no per-job choice.
 *
 * A job row and the job detail carry the value the job was created with:
 * `true` (it plans in phases), `false` (it does its task directly) or `null`
 * (unknown: a job from before the setting, or one on a custom harness image).
 */

/** The expert config key. */
export const PHASES_KEY = 'phases';

/** The job's value as the server returned it; anything but a boolean
 *  (missing, null, an older server's response) is unknown. */
export function jobPhases(job: {phases?: unknown} | null | undefined): boolean | null {
  const value = job?.phases;
  return typeof value === 'boolean' ? value : null;
}

/** The job plans in phases. An unknown job does not count. */
export function plansInPhases(job: {phases?: unknown} | null | undefined): boolean {
  return jobPhases(job) === true;
}

/** The job does its task directly, without phases. An unknown job does not count. */
export function worksWithoutPhases(job: {phases?: unknown} | null | undefined): boolean {
  return jobPhases(job) === false;
}

/** The i18n key of the job detail's "Plans in phases" value ("Yes" / "No");
 *  null when the value is unknown, and the row is then left out. */
export function phasesValueKey(phases: boolean | null): string | null {
  if (phases === null) return null;
  return phases ? 'jobs.detail.phasesYes' : 'jobs.detail.phasesNo';
}

const ON_WORDS = new Set(['on', 'true', 'yes']);

/**
 * Whether an expert config turns phases on. Absent and unreadable values are
 * off. The server also reads the words `on`, `true` and `yes` as on (YAML's
 * `phases: on`), so this does too.
 */
export function expertPlansInPhases(config: Record<string, unknown> | null | undefined): boolean {
  const value = config?.[PHASES_KEY];
  if (typeof value === 'boolean') return value;
  return typeof value === 'string' && ON_WORDS.has(value.trim().toLowerCase());
}

/**
 * The config fragment the expert editor saves for its "Plans in phases"
 * toggle: on writes `phases: true`. Off writes `phases: false` when the stored
 * config has the key (so it replaces the stored value) and nothing when it has
 * none, so an expert that never set it keeps no key.
 */
export function phasesFragment(on: boolean, stored: Record<string, unknown>): Record<string, unknown> {
  if (on) return {[PHASES_KEY]: true};
  return PHASES_KEY in stored ? {[PHASES_KEY]: false} : {};
}
