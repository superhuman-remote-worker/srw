import { Component, Input, ɵresolveComponentResources } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TranslocoPipe, TranslocoTestingModule } from '@jsverse/transloco';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import de from '../../../../assets/i18n/de-DE.json';
import en from '../../../../assets/i18n/en.json';
import {
  CLOUD_FOLDER_KINDS,
  CLOUD_FOLDER_REASONS,
  cloudFolderProblemsFromEvent,
  cloudFolderStateFromEvent,
  cloudFolderStateFromStatus,
} from '../../../core/util/cloud-mount-status';
import { CloudFoldersNoticeComponent } from './cloud-folders-notice.component';

@Component({ selector: 'app-icon', standalone: true, template: '<ng-content />' })
class IconStub {
  @Input() size = '';
}

describe('cloud folder state', () => {
  it('reads unavailable folders and what was left out from the thread', () => {
    const state = cloudFolderStateFromStatus({
      mounts: {
        project: { state: 'unavailable', reason: 'credential_rejected' },
        home: { state: 'mounted', reason: null },
        odd: { state: 'unavailable', reason: '401 Unauthorized' },
      },
      excluded: [{ source_ref: 'r', mount_kind: 'project', reason: 'set_fallback' }],
      notice: 'agent_outdated',
    });
    expect(state.problems).toEqual([
      { name: 'project', path: 'workspace/cloud/project', kind: '', reason: 'credential_rejected' },
      // Anything outside the closed set reads as a mount failure, never raw.
      { name: 'odd', path: 'workspace/cloud/odd', kind: '', reason: 'mount_failed' },
      // A folder left out is named by what it was.
      { name: '', path: '', kind: 'project', reason: 'set_fallback' },
    ]);
    expect(state.agentOutdated).toBe(true);
    expect(state.protected).toBe(false);
  });

  it('names a session\'s only folder workspace/cloud', () => {
    const state = cloudFolderStateFromStatus({
      mounts: { home: { state: 'unavailable', reason: 'timeout' } },
    });
    expect(state.problems[0].path).toBe('workspace/cloud');
  });

  it('knows a protected session that runs without its cloud', () => {
    const state = cloudFolderStateFromStatus({
      mounts: {
        lower: { mount_kind: 'protected_lower', state: 'unavailable', reason: 'credential_rejected' },
      },
    });
    expect(state.protected).toBe(true);
  });

  it('is empty for a session without the record', () => {
    const empty = { problems: [], folders: [], protected: false, agentOutdated: false };
    expect(cloudFolderStateFromStatus(undefined)).toEqual(empty);
    expect(cloudFolderStateFromStatus('nonsense')).toEqual(empty);
  });

  it('reads the agent live event', () => {
    expect(
      cloudFolderProblemsFromEvent({
        mounts: [
          { name: 'project', state: 'mounted' },
          { name: 'reference', state: 'unavailable', reason: 'timeout' },
        ],
        excluded: [{ mount_kind: 'session_folder', reason: 'unbuildable' }],
      }),
    ).toEqual([
      { name: 'reference', path: 'workspace/cloud/reference', kind: '', reason: 'timeout' },
      { name: '', path: '', kind: 'session_folder', reason: 'unbuildable' },
    ]);
    // An agent that reports is up to date.
    expect(cloudFolderStateFromEvent({ mounts: [] }).agentOutdated).toBe(false);
  });

  it('has words for every reason and kind in both languages', () => {
    for (const lang of [en, de]) {
      const folders = (lang as any).chat.cloudFolders;
      for (const reason of CLOUD_FOLDER_REASONS) expect(folders.reason[reason]).toBeTruthy();
      for (const kind of CLOUD_FOLDER_KINDS) expect(folders.notAttachedKind[kind]).toBeTruthy();
      for (const kind of CLOUD_FOLDER_KINDS) expect(folders.kind[kind]).toBeTruthy();
      expect(folders.state.mounted).toBeTruthy();
      expect(folders.state.pending).toBeTruthy();
      expect(folders.review).toBeTruthy();
      expect(folders.protectedTitle).toBeTruthy();
      expect(folders.protectedMeta).toBeTruthy();
    }
  });

  it("lists every attached connector's folder with its level and state", () => {
    const state = cloudFolderStateFromStatus({
      mounts: {
        acme: { mount_kind: 'project', access: 'read_only', state: 'mounted', reason: null },
        outbox: { mount_kind: 'cloud_outbox', access: 'read_write', state: 'pending', reason: null },
      },
      excluded: [
        { source_ref: 'ds-root', mount_kind: 'user_root', reason: 'cloud_not_connected' },
        { source_ref: 'ds-b', mount_kind: 'project', reason: 'grant_unavailable', detail: 'not_ready' },
      ],
    });
    expect(state.folders).toEqual([
      { name: 'acme', path: 'workspace/cloud/acme', kind: 'project', level: 'read_only', state: 'mounted', reason: '', connectorId: '', title: '' },
      { name: 'outbox', path: 'workspace/cloud/outbox', kind: 'cloud_outbox', level: 'read_write', state: 'pending', reason: '', connectorId: '', title: '' },
      // Left out: nothing takes its place, and it says why.
      { name: '', path: '', kind: 'user_root', level: '', state: 'excluded', reason: 'cloud_not_connected', connectorId: 'ds-root', title: '' },
      { name: '', path: '', kind: 'project', level: '', state: 'excluded', reason: 'grant_unavailable', connectorId: 'ds-b', title: '' },
    ]);
    expect(state.problems.map((p) => p.reason)).toEqual(['cloud_not_connected', 'grant_unavailable']);
  });

  it('keeps a protected session with folders left out beside it protected, not broken', () => {
    const state = cloudFolderStateFromStatus({
      mounts: { cloud: { mount_kind: 'protected_lower', access: 'read_only', state: 'mounted' } },
      excluded: [{ source_ref: 'ds-2', mount_kind: 'project', reason: 'beside_protected' }],
    });
    // Its folder is up: the others staying out is no protected outage.
    expect(state.protected).toBe(false);
    expect(state.folders[0].level).toBe('protected');
    expect(state.folders[1].reason).toBe('beside_protected');
  });

  it("keeps what each folder is from the thread's record when the agent reports", () => {
    const before = cloudFolderStateFromStatus({
      mounts: {
        acme: { mount_kind: 'project', access: 'read_write', state: 'pending' },
        outbox: { mount_kind: 'cloud_outbox', access: 'read_write', state: 'pending' },
      },
    });
    const after = cloudFolderStateFromEvent(
      {
        mounts: [
          { name: 'acme', state: 'mounted' },
          { name: 'outbox', state: 'unavailable', reason: 'timeout' },
        ],
        excluded: [{ source_ref: 'ds-3', mount_kind: 'project', reason: 'beside_protected', title: 'Other folder', name: '' }],
      },
      before,
    );
    expect(after.folders.map((f) => [f.name, f.kind, f.level, f.state, f.reason, f.title])).toEqual([
      ['acme', 'project', 'read_write', 'mounted', '', ''],
      ['outbox', 'cloud_outbox', 'read_write', 'unavailable', 'timeout', ''],
      ['', 'project', '', 'excluded', 'beside_protected', 'Other folder'],
    ]);
  });
});

describe('CloudFoldersNoticeComponent', () => {
  let fixture: ComponentFixture<CloudFoldersNoticeComponent>;

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  afterEach(() => TestBed.resetTestingModule());

  async function render(inputs: Record<string, unknown>): Promise<HTMLElement> {
    TestBed.configureTestingModule({
      imports: [
        CloudFoldersNoticeComponent,
        TranslocoTestingModule.forRoot({
          langs: { en },
          translocoConfig: { availableLangs: ['en'], defaultLang: 'en' },
        }),
      ],
    });
    TestBed.overrideComponent(CloudFoldersNoticeComponent, {
      set: { styleUrl: undefined, styles: [''], imports: [TranslocoPipe, IconStub] },
    });
    await TestBed.compileComponents();
    fixture = TestBed.createComponent(CloudFoldersNoticeComponent);
    const inst = fixture.componentInstance as unknown as Record<string, unknown>;
    for (const [k, v] of Object.entries(inputs)) inst[k] = () => v;
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  }

  const text = (root: HTMLElement) => (root.textContent ?? '').replace(/\s+/g, ' ').trim();

  it('names each unavailable folder and why', async () => {
    const root = await render({
      problems: [
        { name: 'project', path: 'workspace/cloud/project', kind: '', reason: 'credential_rejected' },
        { name: '', path: '', kind: 'session_folder', reason: 'unbuildable' },
        { name: '', path: '', kind: '', reason: 'set_fallback' },
      ],
      agentOutdated: false,
    });
    expect(root.querySelector('[role="status"]')?.getAttribute('aria-label')).toBe('Cloud folders');
    expect(text(root)).toContain('Some cloud folders of this session are not available');
    const item = root.querySelector('li')!;
    expect(item.querySelector('.cfn__name')?.textContent).toBe('workspace/cloud/project');
    expect(text(item)).toContain('the cloud refused its credential');
    expect(text(root)).toContain('The session folder was not attached');
    expect(text(root)).toContain('A cloud folder was not attached');
  });

  it('says a protected session runs without its cloud, and why', async () => {
    const root = await render({
      problems: [{ name: 'lower', path: 'workspace/cloud', kind: '', reason: 'credential_rejected' }],
      protectedCloud: true,
      agentOutdated: false,
    });
    const title = root.querySelector('[data-testid="cloud-folders-protected"]');
    expect(text(title as HTMLElement)).toContain('Protected cloud unavailable');
    expect(text(title as HTMLElement)).toContain('the cloud refused its credential');
    expect(text(root)).toContain('nothing it writes reaches the cloud');
    expect(root.querySelector('li')).toBeNull();
  });

  it('says when the agent is too old to manage the folders', async () => {
    const root = await render({ problems: [], agentOutdated: true });
    expect(text(root)).toContain('older than its workspace');
  });

  it('shows nothing for a session without cloud folders', async () => {
    const root = await render({ problems: [], agentOutdated: false });
    expect(root.querySelector('[data-testid="cloud-folders-notice"]')).toBeNull();
  });

  it("shows each folder's binding state, and a failed one names why", async () => {
    const state = cloudFolderStateFromStatus({
      mounts: {
        acme: { mount_kind: 'project', access: 'read_only', state: 'mounted' },
        outbox: { mount_kind: 'cloud_outbox', access: 'read_write', state: 'pending' },
      },
      excluded: [{ source_ref: 'ds-root', mount_kind: 'user_root', reason: 'cloud_not_connected' }],
    });
    const root = await render({ problems: state.problems, folders: state.folders, agentOutdated: false });
    const rows = [...root.querySelectorAll<HTMLElement>('[data-cloud-folder]')];
    expect(rows.map((r) => r.dataset['state'])).toEqual(['mounted', 'pending', 'excluded']);
    expect(text(rows[0])).toBe('workspace/cloud/acme · Project folder Read-only · mounted');
    expect(text(rows[1])).toBe('workspace/cloud/outbox · Session outbox Read-write · connecting…');
    expect(text(rows[2])).toBe(
      'My cloud was not attached · no cloud account yet: connect your cloud under Settings → Connectors',
    );
    expect(rows[2].classList).toContain('cfn__folder--down');
    expect(text(root)).toContain('Some cloud folders of this session are not available');
  });

  it('reads as information when every folder is bound', async () => {
    const state = cloudFolderStateFromStatus({
      mounts: { acme: { mount_kind: 'project', access: 'read_write', state: 'mounted' } },
    });
    const root = await render({ problems: state.problems, folders: state.folders, agentOutdated: false });
    expect(root.querySelector('.cfn')?.classList).toContain('cfn--ok');
    expect(text(root.querySelector('.cfn__title') as HTMLElement)).toBe('Cloud folders');
  });

  it("links a protected session's folder to its review", async () => {
    const state = cloudFolderStateFromStatus({
      mounts: { cloud: { mount_kind: 'protected_lower', access: 'read_only', state: 'mounted' } },
      excluded: [{ source_ref: 'ds-2', mount_kind: 'project', reason: 'beside_protected' }],
    });
    const root = await render({ problems: state.problems, folders: state.folders, protectedCloud: state.protected, agentOutdated: false });
    let reviewed = 0;
    fixture.componentInstance.showReview.subscribe(() => reviewed++);
    const rows = [...root.querySelectorAll<HTMLElement>('[data-cloud-folder]')];
    expect(text(rows[0])).toContain('Protected project folder');
    expect(rows[0].querySelector('[data-level]')?.textContent?.trim()).toBe('Protected');
    (rows[0].querySelector('[data-review]') as HTMLButtonElement).click();
    expect(reviewed).toBe(1);
    expect(text(rows[1])).toContain('not mounted beside a protected folder');
    expect(rows[1].querySelector('[data-review]')).toBeNull();
  });
});
