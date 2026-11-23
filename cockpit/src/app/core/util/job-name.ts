/**
 * What a job is called in lists.
 *
 * A job is created from its task text alone (`description`), so a new job is
 * shown by that text. A few seconds after creation the server names a job a
 * person started with the auxiliary model (orchestrator.services.job_naming),
 * and a person can name or rename it at any time (`title`,
 * PATCH /api/jobs/{id}); the task text never changes with it.
 */

/** Longest name the server accepts (orchestrator.services.job_rename). */
export const JOB_TITLE_MAX_LENGTH = 200;

const SUGGESTION_LENGTH = 80;

/** How long after creation the server may still name a job. Its call has a
 *  30 s limit; a title that has not come by then will not come. */
export const JOB_NAMING_WINDOW_MS = 60_000;

type Nameable = {title?: string | null; description?: string | null};

/**
 * Whether a list row may still get its name from the server: an unnamed root
 * job created within the last minute. Which of those the server really names
 * (by origin and length of the task) is its rule and is not repeated here; a
 * job it leaves alone costs the list two extra reads in its first minute.
 */
export function jobMayStillBeNamed(
  job: {title?: string | null; parent_job_id?: string | null; created_at?: string | null},
  now: number = Date.now(),
): boolean {
  if (job.title || job.parent_job_id || !job.created_at) return false;
  const created = Date.parse(job.created_at);
  return Number.isFinite(created) && now - created < JOB_NAMING_WINDOW_MS && now >= created - 5_000;
}

/** The name to show: the title when the job was named, else the task text. */
export function jobDisplayName(job: Nameable | null | undefined): string {
  return job?.title?.trim() || job?.description || '';
}

/**
 * What the rename editor starts from. A named job starts from its name. An
 * unnamed one starts from the first line of its task text, cut at a word
 * boundary: the whole text is rarely a name, and is often longer than a title
 * may be.
 */
export function jobNameSuggestion(job: Nameable | null | undefined): string {
  const title = job?.title?.trim();
  if (title) return title;
  const firstLine = (job?.description ?? '')
    .split('\n')
    .map((line) => line.replace(/\s+/g, ' ').trim())
    .find((line) => line.length > 0) ?? '';
  if (firstLine.length <= SUGGESTION_LENGTH) return firstLine;
  const cut = firstLine.slice(0, SUGGESTION_LENGTH);
  const lastSpace = cut.lastIndexOf(' ');
  return (lastSpace > SUGGESTION_LENGTH / 2 ? cut.slice(0, lastSpace) : cut).trimEnd();
}
