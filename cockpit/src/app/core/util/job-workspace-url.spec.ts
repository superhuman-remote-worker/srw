import { describe, expect, it } from 'vitest';
import { jobWorkspaceUrl } from './job-workspace-url';

// What the chart renders for every install: the base with a fixed `/srw`.
const FALLBACK = 'https://srw.example.org/git/srw';
// What the API sends on a generator install, whose Gitea admin is srw-admin.
const SERVER = 'https://srw.example.org/git/srw-admin/job-647db6e7';

describe('a job workspace URL', () => {
  it("links the server's repository URL, whose owner is the installation's", () => {
    expect(jobWorkspaceUrl({ repo_name: 'job-647db6e7', gitea_url: SERVER }, FALLBACK)).toBe(SERVER);
    expect(
      jobWorkspaceUrl({ repo_name: 'job-647db6e7', branch_name: 'job/a', gitea_url: SERVER }, FALLBACK),
    ).toBe(`${SERVER}/src/branch/job/a`);
  });

  it('falls back to the configured base for a response without the URL', () => {
    for (const giteaUrl of [undefined, null, '', '  ']) {
      expect(jobWorkspaceUrl({ repo_name: 'job-1', gitea_url: giteaUrl }, FALLBACK)).toBe(`${FALLBACK}/job-1`);
    }
    expect(jobWorkspaceUrl({ repo_name: ' job-1 ', branch_name: 'b' }, FALLBACK)).toBe(
      `${FALLBACK}/job-1/src/branch/b`,
    );
  });

  it('has no link without a repository, even when a URL arrives', () => {
    for (const repo of [null, undefined, '', '   ']) {
      expect(jobWorkspaceUrl({ repo_name: repo, gitea_url: SERVER }, FALLBACK)).toBeNull();
    }
  });

  it('has no link when neither the server nor the configuration names Gitea', () => {
    expect(jobWorkspaceUrl({ repo_name: 'job-1' }, '')).toBeNull();
    expect(jobWorkspaceUrl({ repo_name: 'job-1' }, null)).toBeNull();
  });
});
