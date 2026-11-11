/**
 * What a job is called in lists.
 *
 * A job is created from its task text alone (`description`), so a new job is
 * shown by that text. A person can give it a name afterwards (`title`,
 * PATCH /api/jobs/{id}); the task text never changes with it.
 */

/** Longest name the server accepts (orchestrator.services.job_rename). */
export const JOB_TITLE_MAX_LENGTH = 200;

const SUGGESTION_LENGTH = 80;

type Nameable = {title?: string | null; description?: string | null};

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
