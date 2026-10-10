import { Component, Input, ɵresolveComponentResources } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TranslocoPipe, TranslocoTestingModule } from '@jsverse/transloco';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import de from '../../../../assets/i18n/de-DE.json';
import en from '../../../../assets/i18n/en.json';
import {
  CLOUD_FOLDER_REASONS,
  cloudFolderProblemsFromEvent,
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
      { name: 'project', reason: 'credential_rejected' },
      // Anything outside the closed set reads as a mount failure, never raw.
      { name: 'odd', reason: 'mount_failed' },
      { name: '', reason: 'set_fallback' },
    ]);
    expect(state.agentOutdated).toBe(true);
  });

  it('is empty for a session without the record', () => {
    expect(cloudFolderStateFromStatus(undefined)).toEqual({ problems: [], agentOutdated: false });
    expect(cloudFolderStateFromStatus('nonsense')).toEqual({ problems: [], agentOutdated: false });
  });

  it('reads the agent live event', () => {
    expect(
      cloudFolderProblemsFromEvent({
        mounts: [
          { name: 'project', state: 'mounted' },
          { name: 'reference', state: 'unavailable', reason: 'timeout' },
        ],
        excluded: [],
      }),
    ).toEqual([{ name: 'reference', reason: 'timeout' }]);
  });

  it('has words for every reason in both languages', () => {
    for (const lang of [en, de]) {
      const reasons = (lang as any).chat.cloudFolders.reason as Record<string, string>;
      for (const reason of CLOUD_FOLDER_REASONS) expect(reasons[reason]).toBeTruthy();
    }
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
        { name: 'project', reason: 'credential_rejected' },
        { name: '', reason: 'set_fallback' },
      ],
      agentOutdated: false,
    });
    expect(root.querySelector('[role="status"]')?.getAttribute('aria-label')).toBe('Cloud folders');
    expect(text(root)).toContain('Some cloud folders of this session are not available');
    const item = root.querySelector('li')!;
    expect(item.querySelector('.cfn__name')?.textContent).toBe('workspace/cloud/project');
    expect(text(item)).toContain('the cloud refused its credential');
    expect(text(root)).toContain('A cloud folder was not attached');
  });

  it('says when the agent is too old to manage the folders', async () => {
    const root = await render({ problems: [], agentOutdated: true });
    expect(text(root)).toContain('older than its workspace');
  });

  it('shows nothing when every folder came up', async () => {
    const root = await render({ problems: [], agentOutdated: false });
    expect(root.querySelector('[data-testid="cloud-folders-notice"]')).toBeNull();
  });
});
