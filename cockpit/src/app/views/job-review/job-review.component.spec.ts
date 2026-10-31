import { CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { TranslocoPipe, TranslocoTestingModule } from '@jsverse/transloco';
import { of } from 'rxjs';
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import en from '../../../assets/i18n/en.json';
import { Datasource, Job, PullRequestStatus } from '../../core/models/api.model';
import { ApiService } from '../../core/services/api.service';
import { DataService } from '../../core/services/data.service';
import {
  JobReviewComponent,
  pullRequestFromJob,
  repositoryBranchUrl,
  repositoryWebUrl,
  selectDeliveryRepository,
} from './job-review.component';

const JOB_ID = '29c28492-df7c-4eb3-847f-38892557ac4e';
const PR_URL = 'https://github.com/Acme/WidgetShop/pull/1';
const OPEN_STATUS: PullRequestStatus = {
  forge: 'github',
  repo: 'Acme/WidgetShop',
  number: 1,
  url: PR_URL,
  state: 'open',
  head: 'design/hotel-rheinland-theme',
  base: 'main',
  draft: false,
};

function repository(id: string, connectionUrl: string): Datasource {
  return {
    id,
    name: id,
    description: null,
    type: 'repository',
    connection_url: connectionUrl,
    cli_hint: null,
    default_branch: 'main',
    config: { forge: 'github' },
    job_id: JOB_ID,
    created_at: '',
    updated_at: '',
  };
}

function reviewJob(context: Job['context'], overrides: Partial<Job> = {}): Job {
  return {
    id: JOB_ID,
    description: 'Design the Hotel Rheinland theme',
    config_name: 'worker_base',
    status: 'pending_review',
    created_at: '2026-08-14T08:00:00Z',
    repo_name: 'job-29c28492',
    branch_name: null,
    context,
    ...overrides,
  };
}

describe('job-review delivery URL helpers', () => {
  it('turns HTTPS and SSH clone URLs into credential-free repository pages', () => {
    expect(repositoryWebUrl('https://token@github.com/acme/widget.git')).toBe(
      'https://github.com/acme/widget',
    );
    expect(repositoryWebUrl('git@github.com:acme/widget.git')).toBe(
      'https://github.com/acme/widget',
    );
    expect(repositoryWebUrl('ssh://git@git.example.test/acme/widget.git')).toBe(
      'https://git.example.test/acme/widget',
    );
  });

  it('builds forge-specific branch pages without losing slash-delimited refs', () => {
    const repoUrl = 'https://github.com/acme/widget.git';
    expect(repositoryBranchUrl(repoUrl, 'design/hotel theme', 'github')).toBe(
      'https://github.com/acme/widget/tree/design/hotel%20theme',
    );
    expect(repositoryBranchUrl(repoUrl, 'feature/a', 'gitlab')).toBe(
      'https://github.com/acme/widget/-/tree/feature/a',
    );
    expect(repositoryBranchUrl(repoUrl, 'feature/a', 'gitea')).toBe(
      'https://github.com/acme/widget/src/branch/feature/a',
    );
  });

  it('reads the persisted PR from raw JSONB text and rejects unsafe links', () => {
    const pullRequest = pullRequestFromJob(
      reviewJob(
        JSON.stringify({
          pull_request: {
            forge: 'github',
            repo: 'Acme/WidgetShop',
            number: 1,
            url: PR_URL,
            head: 'design/hotel-rheinland-theme',
            base: 'main',
          },
        }),
      ),
    );
    expect(pullRequest?.url).toBe(PR_URL);
    expect(pullRequest?.head).toBe('design/hotel-rheinland-theme');

    const unsafe = pullRequestFromJob(
      reviewJob({
        pull_request: {
          forge: 'github',
          repo: 'acme/widget',
          number: 1,
          url: 'javascript:alert(1)',
          head: 'feature/a',
          base: 'main',
        },
      }),
    );
    expect(unsafe).toBeNull();
  });

  it('matches the source connector to the PR instead of taking an unrelated repo', () => {
    const pullRequest = pullRequestFromJob(
      reviewJob({
        pull_request: {
          forge: 'github',
          repo: 'Acme/WidgetShop',
          number: 1,
          url: PR_URL,
          head: 'design/hotel-rheinland-theme',
          base: 'main',
        },
      }),
    );
    const selected = selectDeliveryRepository(
      [
        repository('unrelated', 'https://github.com/acme/other.git'),
        repository('widget-shop', 'https://github.com/Acme/WidgetShop.git'),
      ],
      pullRequest,
    );
    expect(selected?.id).toBe('widget-shop');
  });

  it('does not substitute the same owner/repo from another forge host', () => {
    const pullRequest = pullRequestFromJob(
      reviewJob({
        pull_request: {
          forge: 'github',
          repo: 'Acme/WidgetShop',
          number: 1,
          url: PR_URL,
          head: 'design/hotel-rheinland-theme',
          base: 'main',
        },
      }),
    );
    const selected = selectDeliveryRepository(
      [
        repository(
          'enterprise-lookalike',
          'https://github.enterprise.test/Acme/WidgetShop.git',
        ),
        repository(
          'github-delivery',
          'https://github.com/Acme/WidgetShop.git',
        ),
      ],
      pullRequest,
    );

    expect(selected?.id).toBe('github-delivery');
    expect(
      selectDeliveryRepository(
        [
          repository(
            'enterprise-lookalike',
            'https://github.enterprise.test/Acme/WidgetShop.git',
          ),
        ],
        pullRequest,
      ),
    ).toBeNull();
  });

  it('does not label an unrelated connector as the source when several are attached', () => {
    const pullRequest = pullRequestFromJob(
      reviewJob({
        pull_request: {
          forge: 'github',
          repo: 'Acme/WidgetShop',
          number: 1,
          url: PR_URL,
          head: 'design/hotel-rheinland-theme',
          base: 'main',
        },
      }),
    );
    expect(
      selectDeliveryRepository(
        [
          repository('first', 'https://github.com/acme/first.git'),
          repository('second', 'https://github.com/acme/second.git'),
        ],
        pullRequest,
      ),
    ).toBeNull();
  });
});

describe('JobReviewComponent delivery section', () => {
  let fixture: ComponentFixture<JobReviewComponent>;
  let api: Record<string, ReturnType<typeof vi.fn>>;
  let router: {navigate: ReturnType<typeof vi.fn>};

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  afterEach(() => TestBed.resetTestingModule());

  async function render(
    context: Job['context'] = JSON.stringify({
      pull_request: {
        forge: 'github',
        repo: 'Acme/WidgetShop',
        number: 1,
        url: PR_URL,
        head: 'design/hotel-rheinland-theme',
        base: 'main',
      },
    }),
    liveStatus: PullRequestStatus | null = OPEN_STATUS,
    workspaceContract: Job['workspace_contract'] | undefined = undefined,
    jobOverrides: Partial<Job> = {},
  ): Promise<HTMLElement> {
    const job = reviewJob(context, jobOverrides);
    job.workspace_contract = workspaceContract;
    api = {
      getJob: vi.fn().mockReturnValue(of(job)),
      getFrozenJobData: vi.fn().mockReturnValue(of({ summary: 'Theme delivered' })),
      getJobPullRequestStatus: vi.fn().mockReturnValue(of(liveStatus)),
      createJobReviewSession: vi.fn().mockReturnValue(of({
        job_id: JOB_ID,
        thread_id: 'thread-review',
        status: 'created',
      })),
      getJobDatasources: vi
        .fn()
        .mockReturnValue(
          of([repository('widget-shop', 'https://github.com/Acme/WidgetShop.git')]),
        ),
      ensureWorkspaceAccess: vi.fn().mockReturnValue(of({ status: 'ready' })),
    };
    router = {navigate: vi.fn().mockResolvedValue(true)};

    TestBed.configureTestingModule({
      imports: [
        JobReviewComponent,
        TranslocoTestingModule.forRoot({
          langs: { en },
          translocoConfig: { availableLangs: ['en'], defaultLang: 'en' },
        }),
      ],
      providers: [
        { provide: ApiService, useValue: api },
        { provide: DataService, useValue: { currentJobId: signal(JOB_ID) } },
        { provide: Router, useValue: router },
      ],
    });
    // Signal-input metadata on the design-system children is unavailable in
    // this Vitest JIT pipeline. Keep the real host template and translation
    // pipe, while treating its child elements as inert render containers.
    TestBed.overrideComponent(JobReviewComponent, {
      set: { imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA] },
    });
    await TestBed.compileComponents();
    fixture = TestBed.createComponent(JobReviewComponent);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    expect(api.getJobDatasources).toHaveBeenCalledWith(JOB_ID);
    if (pullRequestFromJob(job)) {
      expect(api.getJobPullRequestStatus).toHaveBeenCalledWith(JOB_ID);
    } else {
      expect(api.getJobPullRequestStatus).not.toHaveBeenCalled();
    }
    return fixture.nativeElement as HTMLElement;
  }

  it('links the real delivery and labels the scratch repository as a job workspace', async () => {
    const root = await render();
    const text = (root.textContent ?? '').replace(/\s+/g, ' ').trim();

    expect(text).toContain('Delivery');
    expect(text).toContain('Source repository');
    expect(text).toContain('Branch design/hotel-rheinland-theme');
    expect(text).toContain('Pull request #1 · Open');
    expect(text).toContain('Review in session');
    expect(text).toContain('fresh sandbox');
    expect(text).toContain('read-only session');
    expect(text).toContain('Job workspace');
    expect(text).not.toContain('Browse workspace in Gitea');

    expect(fixture.componentInstance.sourceRepositoryUrl()).toBe(
      'https://github.com/Acme/WidgetShop',
    );
    expect(fixture.componentInstance.deliveryBranchUrl()).toBe(
      'https://github.com/Acme/WidgetShop/tree/design/hotel-rheinland-theme',
    );
    expect(fixture.componentInstance.pullRequest()?.url).toBe(PR_URL);
    expect(fixture.componentInstance.getJobWorkspaceUrl()).toMatch(/\/job-29c28492$/);
  });

  it('has no workspace link when the job has no repository', async () => {
    for (const repo of [null, '', '   ']) {
      TestBed.resetTestingModule();
      await render(undefined, OPEN_STATUS, undefined, {repo_name: repo as unknown as string});
      expect(fixture.componentInstance.getJobWorkspaceUrl()).toBeNull();
    }
  });

  it('links the workspace when the job has a repository', async () => {
    await render(undefined, OPEN_STATUS, undefined, {repo_name: 'my-repo'});
    expect(fixture.componentInstance.getJobWorkspaceUrl()).toMatch(/\/my-repo$/);
  });

  it('creates the server-derived session and navigates to it', async () => {
    await render();

    fixture.componentInstance.reviewInSession();
    await fixture.whenStable();

    expect(api['createJobReviewSession']).toHaveBeenCalledWith(JOB_ID);
    expect(router.navigate).toHaveBeenCalledWith(['/sessions', 'thread-review']);
  });

  it('keeps the PR link and states when the live read is unavailable', async () => {
    const root = await render(undefined, null);
    const text = (root.textContent ?? '').replace(/\s+/g, ' ').trim();

    expect(text).toContain('Pull request #1 · Status unavailable');
    expect(fixture.componentInstance.pullRequest()?.url).toBe(PR_URL);
  });

  it('renders blocked/undelivered as distinct from completed or failed', async () => {
    const root = await render(
      {},
      null,
      undefined,
      {status: 'cancelled', completion_outcome_kind: 'blocked_undelivered'},
    );
    const text = (root.textContent ?? '').replace(/\s+/g, ' ').trim();

    expect(text).toContain('Blocked / undelivered');
    expect(text).not.toContain('Failed');
    expect(text).not.toContain('Completed');
  });

  it('does not mislabel the connector default as the historical job delivery branch', async () => {
    const root = await render({ cloud_baseline: { status: 'captured' } });
    const text = (root.textContent ?? '').replace(/\s+/g, ' ').trim();

    expect(text).toContain('Source repository');
    expect(text).toContain('Job workspace');
    expect(text).not.toContain('Branch main');
    expect(text).not.toContain('Pull request #');
  });

  it('shows only the safe server-owned workspace tier projection', async () => {
    const root = await render(undefined, OPEN_STATUS, {
      requested_backend: 'vm',
      assigned_backend: 'vm',
      effective_backend: 'vm',
      state: 'ready',
      failure: null,
      stale_backend: 'sandbox',
    });
    const text = (root.textContent ?? '').replace(/\s+/g, ' ').trim();

    expect(text).toContain('Workspace: requested vm · assigned vm · effective vm');
    expect(root.querySelector('.workspace-contract')?.getAttribute('title')).toBe(
      'Workspace state: ready · detail: none',
    );
    expect(text).not.toContain('ssh_host');
    expect(text).not.toContain('private.svc');
  });

  it("offers the outbox re-sync on a cloud project's job too, with what the last copy did", async () => {
    const root = await render(
      {job_outbox: {state: 'delivered', files: 3, shared: false, unshared: 'rate_limited'}},
      null,
      undefined,
      {
        cloud_review_mode: 'diff',
        diff_status: 'accepted',
        exported_at: '2026-10-10T20:00:00Z',
        exported_folder_url: 'https://cloud.example/f/1',
      },
    );
    const section = root.querySelector('.cloud-folder') as HTMLElement;
    const text = (section.textContent ?? '').replace(/\s+/g, ' ').trim();
    expect(text).toContain("3 file(s) in the job's cloud folder.");
    expect(section.querySelector('[data-outbox-unshared]')?.textContent?.trim()).toBe(
      en.jobReview.outbox.unshared.rate_limited,
    );
    expect(text).toContain(en.jobReview.links.openCloudFolder);
    expect(text).toContain(en.jobReview.actions.resyncCloudFolder);
    expect(fixture.componentInstance.cloudAction()).toBe('open');
  });

  it('says why the copy into the outbox failed', async () => {
    const root = await render({job_outbox: {state: 'failed', reason: 'folder_failed', detail: 'boom'}}, null);
    const state = root.querySelector('[data-outbox-state]') as HTMLElement;
    expect(state.dataset['state']).toBe('failed');
    expect((state.textContent ?? '').replace(/\s+/g, ' ').trim()).toBe(
      "The copy into the job's cloud folder failed: the cloud folder could not be created.",
    );
    // The server's own words never reach the page.
    expect(root.textContent).not.toContain('boom');
  });

  it('says a reviewed project folder copy has no changes, and why', async () => {
    const root = await render(
      {cloud_checkout: {access: 'reviewed_write_back', capture: {state: 'no_changes'}}},
      null,
      undefined,
      {cloud_review_mode: 'diff', diff_status: null},
    );
    expect(root.querySelector('[data-no-project-folder-changes]')?.textContent?.trim()).toBe(
      en.jobReview.projectFolder.noChanges,
    );
  });

  it('shows no project folder note for a job without a reviewed copy', async () => {
    const root = await render(undefined, OPEN_STATUS, undefined, {cloud_review_mode: 'open_folder'});
    expect(root.querySelector('[data-no-project-folder-changes]')).toBeNull();
  });

  it('keeps a cancelled VM on the non-review screen without a workspace tooltip', async () => {
    const root = await render(undefined, OPEN_STATUS, {
      requested_backend: 'vm', assigned_backend: 'vm', effective_backend: null,
      state: 'waiting', failure: 'vm_runtime_not_ready',
    }, {status: 'cancelled'});

    expect(root.querySelector('.not-review-state')).not.toBeNull();
    expect(root.querySelector('.review-content')).toBeNull();
    expect(root.querySelector('.workspace-contract')).toBeNull();
  });
});
