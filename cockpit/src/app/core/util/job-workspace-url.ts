/** The repository fields a job row carries, list and detail alike. */
export interface JobWorkspaceRepo {
  repo_name?: string | null;
  branch_name?: string | null;
  /** The repository's Gitea page, built by the orchestrator under its real owner. */
  gitea_url?: string | null;
}

/**
 * The Gitea page of a job's workspace repository, on its branch when it has one.
 *
 * The orchestrator's `gitea_url` is the source of truth: only it knows the
 * account that owns managed repositories (`GITEA_ADMIN_USER`, `srw-admin` on a
 * generator install). `fallbackBase` (the chart's `giteaUrl`, whose owner is a
 * fixed `srw`) is used only for a response that predates that field.
 */
export function jobWorkspaceUrl(
  job: JobWorkspaceRepo,
  fallbackBase: string | null | undefined,
): string | null {
  const repoName = job.repo_name?.trim();
  // No repository means no workspace page: a guessed name is a dead link.
  if (!repoName) return null;
  const repoUrl = job.gitea_url?.trim() || (fallbackBase ? `${fallbackBase}/${repoName}` : null);
  if (!repoUrl) return null;
  return job.branch_name ? `${repoUrl}/src/branch/${job.branch_name}` : repoUrl;
}
