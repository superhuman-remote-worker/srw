import {beforeAll, describe, expect, it, vi} from 'vitest';
import {CUSTOM_ELEMENTS_SCHEMA, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {of, Subject, throwError} from 'rxjs';
import {MainCloudSettingsComponent} from './main-cloud-settings.component';
import {SettingsService} from '../../../core/services/settings.service';
import type {
  MainCloudBackgroundJobs,
  MainCloudPage,
  MainCloudRetention,
} from '../../../core/models/main-cloud.model';
// The API's own response (tests/test_b04_lane_m_main_cloud_settings_routes.py
// pins it), so this spec renders what the server really sends.
import fixture from '../../../core/models/fixtures/main-cloud.json';
// The real catalogue, so these specs also prove the keys exist.
import en from '../../../../assets/i18n/en.json';

const PAGE = fixture as unknown as MainCloudPage;

function mount(response: unknown = of(structuredClone(PAGE))) {
  const service = {getMainCloud: vi.fn(() => response)};
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    imports: [
      MainCloudSettingsComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        preloadLangs: true,
      }),
    ],
    providers: [{provide: SettingsService, useValue: service}],
  });
  // The primitives stay inert (their signal inputs are not wired in this
  // harness); their text renders.
  TestBed.overrideComponent(MainCloudSettingsComponent, {
    set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  const fixture = TestBed.createComponent(MainCloudSettingsComponent);
  fixture.detectChanges();
  return {fixture, host: fixture.nativeElement as HTMLElement, service};
}

const text = (element: Element | null) => (element?.textContent ?? '').replace(/\s+/g, ' ').trim();
const cell = (host: HTMLElement, row: string, provider: string) =>
  host.querySelector<HTMLElement>(`[data-row="${row}"] [data-cell="${provider}"]`)!;

const JOBS = en.settings.cloud.jobs;
/** An ISO time `minutes` ago (plus a margin, so a slow test never rounds down). */
const ago = (minutes: number) => new Date(Date.now() - minutes * 60_000 - 10_000).toISOString();
/** The fixture (a bundled Nextcloud: cron, chart retention) with `jobs` over it. */
function withJobs(jobs: Partial<MainCloudBackgroundJobs>, retention?: MainCloudRetention) {
  const page = structuredClone(PAGE);
  page.background_jobs = {...page.background_jobs!, ...jobs};
  if (retention !== undefined) page.retention = retention;
  return page;
}
const jobsSection = (host: HTMLElement) =>
  host.querySelector<HTMLElement>('[data-section="background-jobs"]');
const fact = (host: HTMLElement, name: string) =>
  jobsSection(host)!.querySelector<HTMLElement>(`[data-fact="${name}"]`);

describe('MainCloudSettingsComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('loads the page when opened', () => {
    const {service} = mount();
    expect(service.getMainCloud).toHaveBeenCalledTimes(1);
  });

  it('shows the provider, its public URL, installation and health', () => {
    const {host} = mount();
    expect(text(host.querySelector('[data-section="status"]'))).toContain('Nextcloud');
    expect(text(host.querySelector('[data-section="status"]'))).toContain('healthy (12.3 ms)');
    const link = host.querySelector<HTMLAnchorElement>('[data-section="facts"] a')!;
    expect(link.getAttribute('href')).toBe('https://cloud.localhost');
    expect(text(host.querySelector('[data-fact="installation"]'))).toContain(
      PAGE.provider.backend_instance_id!,
    );
    expect(text(host.querySelector('[data-fact="configuration"]'))).toContain(
      en.settings.cloud.helm.matches,
    );
  });

  it('has no form: nothing on the page can change the configuration', () => {
    const {host} = mount();
    expect(host.querySelectorAll('input, select, textarea, form').length).toBe(0);
    expect(host.querySelectorAll('app-button').length).toBe(1); // refresh only
  });

  it('renders the design table: one row per combination, one cell per provider', () => {
    const {host} = mount();
    const headers = [...host.querySelectorAll('thead th[data-provider]')].map((th) =>
      th.getAttribute('data-provider'),
    );
    expect(headers).toEqual(['nextcloud', 'opencloud']);
    const rows = [...host.querySelectorAll('tbody tr')].map((tr) => tr.getAttribute('data-row'));
    expect(rows).toEqual([
      'cloud_folder/project/read_only',
      'cloud_folder/project/read_write',
      'cloud_folder/project/protected',
      'cloud_folder/user_root/read_write',
      'cloud_folder/user_root/read_only',
      'cloud_folder/user_root/protected',
      'cloud_folder_checkout/project/read_only',
      'cloud_folder_checkout/project/reviewed_write_back',
      'cloud_outbox/-/read_write',
    ]);
  });

  it('says how a level is enforced, where, and why one is missing', () => {
    const {host} = mount();
    const protectedNc = cell(host, 'cloud_folder/project/protected', 'nextcloud');
    expect(protectedNc.getAttribute('data-status')).toBe('offered');
    expect(text(protectedNc)).toContain('sandbox');
    expect(text(protectedNc)).toContain('reader grant');

    const protectedOc = cell(host, 'cloud_folder/project/protected', 'opencloud');
    expect(protectedOc.getAttribute('data-status')).toBe('unsupported');
    expect(text(protectedOc)).toContain('Nextcloud only');

    const rootRo = cell(host, 'cloud_folder/user_root/read_only', 'nextcloud');
    expect(text(rootRo)).toContain('app token');

    // A project folder's reader: its own account, offered on a VM too.
    const readerNc = cell(host, 'cloud_folder/project/read_only', 'nextcloud');
    expect(readerNc.getAttribute('data-status')).toBe('offered');
    expect(text(readerNc)).toContain('reader account');
    expect(text(readerNc)).toContain('vm');

    // A checkout is offered at both levels on both providers.
    for (const provider of ['nextcloud', 'opencloud']) {
      const writeBack = cell(host, 'cloud_folder_checkout/project/reviewed_write_back', provider);
      expect(writeBack.getAttribute('data-status')).toBe('offered');
      expect(text(writeBack)).toContain('accepts');
      expect(cell(host, 'cloud_folder_checkout/project/read_only', provider)
        .getAttribute('data-status')).toBe('offered');
    }
  });

  it('marks the active provider', () => {
    const {host} = mount();
    const active = host.querySelector('thead th.active')!;
    expect(active.getAttribute('data-provider')).toBe('nextcloud');
    expect(text(active)).toContain(en.settings.cloud.activeBadge);
  });

  it('explains Helm drift and the replacement confirmation', () => {
    const page = structuredClone(PAGE);
    page.configuration.helm.state = 'differs';
    const {host} = mount(of(page));
    const note = host.querySelector('[data-helm="differs"]')!;
    expect(text(note)).toContain('cloud.replaceInstallation');
    expect(text(note)).toContain(PAGE.provider.backend_instance_id!);
  });

  it('labels a combination it does not know by its id', () => {
    const page = structuredClone(PAGE);
    page.matrix.rows = [{...page.matrix.rows[0], connector_type: 'cloud_drive'}];
    const {host} = mount(of(page));
    expect(text(host.querySelector('tbody th'))).toContain('cloud_drive');
  });

  it('offers a retry when the page cannot load', () => {
    const {host, service, fixture} = mount(throwError(() => new Error('nope')));
    expect(text(host.querySelector('[role="alert"]'))).toContain(en.settings.cloud.loadFailed);
    service.getMainCloud.mockReturnValue(of(structuredClone(PAGE)));
    fixture.componentInstance.load();
    fixture.detectChanges();
    expect(host.querySelector('[role="alert"]')).toBeNull();
    expect(host.querySelector('table')).not.toBeNull();
  });

  describe('background jobs (D13)', () => {
    it('shows a healthy cron: its mode, last run and the retention the chart pins', () => {
      const lastRun = ago(3);
      const {host} = mount(of(withJobs({last_run_at: lastRun})));

      expect(text(host)).toContain(JOBS.title);
      expect(text(fact(host, 'mode'))).toBe('cron');
      const time = fact(host, 'last-run')!.querySelector('time')!;
      expect(text(time)).toBe('3 minutes ago');
      expect(time.getAttribute('datetime')).toBe(lastRun);
      expect(time.getAttribute('title')).toBe(new Date(lastRun).toLocaleString());
      expect(text(fact(host, 'cron-container'))).toBe(JOBS.cronOn);
      expect(text(fact(host, 'trashbin'))).toBe('disabled');
      expect(text(fact(host, 'versions'))).toBe('disabled');
      const note = text(host.querySelector('[data-note="retention"]'));
      expect(note).toContain(JOBS.retentionChart);
      expect(note).toContain(JOBS.retentionDisabled);

      expect(jobsSection(host)!.classList.contains('stale')).toBe(false);
      expect(text(fact(host, 'last-run'))).not.toContain(JOBS.stale);
      expect(host.querySelector('[data-jobs="stale"]')).toBeNull();
    });

    it('warns when cron has missed its runs', () => {
      const {host} = mount(of(withJobs({last_run_at: ago(40), stale: true})));

      expect(jobsSection(host)!.classList.contains('stale')).toBe(true);
      expect(text(fact(host, 'last-run'))).toContain('40 minutes ago');
      expect(text(fact(host, 'last-run'))).toContain(JOBS.stale);
      expect(text(host.querySelector('[data-jobs="stale"]'))).toBe(JOBS.staleNote);
    });

    it('says cron never ran, and warns', () => {
      const {host} = mount(of(withJobs({last_run_at: null, stale: true})));

      expect(fact(host, 'last-run')!.querySelector('time')).toBeNull();
      expect(text(fact(host, 'last-run'))).toContain(JOBS.never);
      expect(text(fact(host, 'last-run'))).toContain(JOBS.stale);
      expect(host.querySelector('[data-jobs="stale"]')).not.toBeNull();
    });

    it("leaves an external Nextcloud's retention to its administrator", () => {
      const page = withJobs(
        {mode: 'ajax', last_run_at: ago(120), stale: false, cron_container: null},
        {source: 'external'},
      );
      const {host} = mount(of(page));

      expect(text(fact(host, 'mode'))).toContain(JOBS.ajaxNote);
      expect(text(fact(host, 'last-run'))).toBe('2 hours ago');
      expect(fact(host, 'cron-container')).toBeNull();
      expect(fact(host, 'trashbin')).toBeNull();
      expect(fact(host, 'versions')).toBeNull();
      expect(text(fact(host, 'retention'))).toBe(JOBS.retentionExternal);
      expect(host.querySelector('[data-note="retention"]')).toBeNull();
      expect(host.querySelector('[data-jobs="stale"]')).toBeNull();
    });

    it('shows an unreadable status as unknown, without a warning', () => {
      const {host} = mount(of(withJobs({mode: null, last_run_at: null, stale: false})));

      expect(text(fact(host, 'mode'))).toBe(en.settings.cloud.unknown);
      expect(text(fact(host, 'last-run'))).toBe(en.settings.cloud.unknown);
      expect(host.querySelector('[data-jobs="stale"]')).toBeNull();
    });

    it('renders nothing for an older orchestrator or a provider without background jobs', () => {
      const older = structuredClone(PAGE);
      delete older.background_jobs;
      delete older.retention;
      const openCloud = {...structuredClone(PAGE), background_jobs: null, retention: null};

      for (const page of [older, openCloud]) {
        const {host} = mount(of(page));
        expect(jobsSection(host)).toBeNull();
        expect(text(host)).not.toContain(JOBS.title);
        expect(host.querySelector('[data-note="retention"]')).toBeNull();
        expect(host.querySelector('table')).not.toBeNull();
      }
    });
  });

  it('shows a loading state until the answer arrives', () => {
    const pending = new Subject<MainCloudPage>();
    const {host, fixture} = mount(pending);
    expect(text(host)).toContain(en.settings.cloud.loading);
    pending.next(structuredClone(PAGE));
    fixture.detectChanges();
    expect(host.querySelector('table')).not.toBeNull();
  });
});
