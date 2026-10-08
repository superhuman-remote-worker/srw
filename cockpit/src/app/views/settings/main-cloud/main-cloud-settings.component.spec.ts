import {beforeAll, describe, expect, it, vi} from 'vitest';
import {CUSTOM_ELEMENTS_SCHEMA, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {of, Subject, throwError} from 'rxjs';
import {MainCloudSettingsComponent} from './main-cloud-settings.component';
import {SettingsService} from '../../../core/services/settings.service';
import type {MainCloudPage} from '../../../core/models/main-cloud.model';
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

    const plannedRo = cell(host, 'cloud_folder/project/read_only', 'nextcloud');
    expect(text(plannedRo)).toContain('planned (slice 5)');
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

  it('shows a loading state until the answer arrives', () => {
    const pending = new Subject<MainCloudPage>();
    const {host, fixture} = mount(pending);
    expect(text(host)).toContain(en.settings.cloud.loading);
    pending.next(structuredClone(PAGE));
    fixture.detectChanges();
    expect(host.querySelector('table')).not.toBeNull();
  });
});
