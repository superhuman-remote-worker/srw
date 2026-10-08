import {beforeAll, describe, expect, it, vi} from 'vitest';
import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {ConnectorDriversPageComponent, driverAnchor} from './connector-drivers-page.component';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {ConnectorDriver, ConnectorDriverMatrix} from '../../core/models/connector-driver.model';
// The API's own response for the built-in drivers
// (tests/test_connector_capability_matrix.py pins it).
import fixture from '../../core/models/fixtures/connector-drivers.json';
// The real catalogue, so these specs also prove the keys exist.
import en from '../../../assets/i18n/en.json';

const matrix = fixture as unknown as ConnectorDriverMatrix;
const page = en.connectorDrivers;

/** A driver outside the trusted list, as D6 will register one. */
const CUSTOM: ConnectorDriver = {
  ...matrix.drivers.find((driver) => driver.name === 'srw.generic/v1')!,
  name: 'community.ticketing/v1',
  title: 'Ticketing',
  legacy_type: null,
  egress: {
    declared: {rules: [{host: '${config.host}', ports: [443], protocol: 'tcp'}], needs_dns: 'a name'},
    enforced: {status: 'not_enforced', reason: 'driver_hosting_not_available'},
    installation: {status: 'not_enforced', reason: 'driver_hosting_not_available'},
  },
  trust: {tier: 'custom', trusted: false, image: 'ghcr.io/acme/ticketing:1', claims_declared_by_author: true},
};

function mount(drivers: ConnectorDriver[] | null, loadFailed = false) {
  const service = {
    drivers: signal(drivers),
    loadFailed: signal(loadFailed),
    load: vi.fn(),
  };
  TestBed.configureTestingModule({
    imports: [
      ConnectorDriversPageComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        preloadLangs: true,
      }),
    ],
    providers: [{provide: ConnectorDriversService, useValue: service}],
  });
  // The primitives stay inert (their signal inputs are not wired in this
  // harness, see helm-managed-badge.component.spec.ts); their text renders.
  TestBed.overrideComponent(ConnectorDriversPageComponent, {
    set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  const fixture = TestBed.createComponent(ConnectorDriversPageComponent);
  fixture.detectChanges();
  return {fixture, host: fixture.nativeElement as HTMLElement, service};
}

const card = (host: HTMLElement, name: string) =>
  host.querySelector<HTMLElement>(`[data-driver="${name}"]`)!;
const text = (element: Element | null) => (element?.textContent ?? '').replace(/\s+/g, ' ').trim();

describe('ConnectorDriversPageComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('refreshes the matrix when opened', () => {
    const {service} = mount(matrix.drivers);
    expect(service.load).toHaveBeenCalledWith(true);
  });

  it('lists every installed driver with its access levels and what enforces each', () => {
    const {host} = mount(matrix.drivers);
    const cards = [...host.querySelectorAll('[data-driver]')].map((el) => el.getAttribute('data-driver'));
    expect(cards).toEqual(matrix.drivers.map((driver) => driver.name));
    for (const driver of matrix.drivers) {
      const levels = [...card(host, driver.name).querySelectorAll('.level')];
      expect(levels.map((el) => el.getAttribute('data-level'))).toEqual(
        driver.access_levels.map((level) => level.id),
      );
      driver.access_levels.forEach((level, i) => {
        const line = text(levels[i].querySelector('.enforced-by'));
        expect(line).toBe(`${page.enforcedBy} ${level.enforced_by}`);
        expect(text(levels[i].querySelector('.level-head'))).toContain(
          level.advisory ? page.advisory : page.enforced,
        );
      });
      if (driver.access_levels.length === 0) {
        expect(text(card(host, driver.name).querySelector('[data-section="access"]'))).toContain(
          page.noAccessLevels,
        );
      }
    }
    expect(card(host, 'srw.kb/v1').id).toBe(driverAnchor('srw.kb/v1'));
  });

  it('shows built-ins as built-in and trusted, running inside SRW', () => {
    const {host} = mount(matrix.drivers);
    const postgres = card(host, 'srw.postgresql/v1');
    expect(text(postgres.querySelector('.driver-badges'))).toContain(page.trust.builtin);
    expect(text(postgres.querySelector('.driver-badges'))).toContain(page.holdsCredentials);
    expect(text(postgres)).toContain(page.shipsWithSrw);
    expect(text(postgres.querySelector('[data-egress="declared"]'))).toContain(page.egressNone);
    expect(text(postgres.querySelector('[data-egress="enforced"]'))).toContain(
      page.egressReason.runs_in_srw_process,
    );
    expect(text(postgres.querySelector('[data-egress="installation"]'))).toContain(
      page.egressReason.runs_in_srw_process,
    );
    expect(postgres.querySelector('.claim-source')).toBeNull();
    expect(text(card(host, 'srw.kb/v1').querySelector('.driver-badges'))).toContain(page.alwaysReadOnly);
    expect(text(card(host, 'srw.mcp/v1').querySelector('.tools'))).toBe(page.toolsDiscovered);
  });

  it("marks every claim of a driver outside the trusted list as its author's", () => {
    const {host} = mount([CUSTOM]);
    const custom = card(host, CUSTOM.name);
    expect(text(custom.querySelector('[data-claims="author"]'))).toBe(page.authorNote);
    // Access levels, backends, credentials and egress each say so.
    expect(custom.querySelectorAll('.claim-source')).toHaveLength(4);
    expect(text(custom.querySelector('.driver-badges'))).toContain(page.trust.custom);
    expect(text(custom)).toContain('ghcr.io/acme/ticketing:1');
    expect(text(custom.querySelector('[data-egress="declared"]'))).toContain('${config.host}:443/tcp');
    expect(text(custom.querySelector('[data-egress="enforced"]'))).toContain(
      page.egressReason.driver_hosting_not_available,
    );
  });

  it('filters by title, name or stored type', () => {
    const {fixture, host} = mount(matrix.drivers);
    fixture.componentInstance.query.set('ssh_key');
    fixture.detectChanges();
    expect([...host.querySelectorAll('[data-driver]')].map((el) => el.getAttribute('data-driver'))).toEqual([
      'srw.ssh-key/v1',
    ]);
    fixture.componentInstance.query.set('nothing-like-this');
    fixture.detectChanges();
    expect(text(host)).toContain(page.noMatch);
  });

  it('offers a retry when the matrix cannot be read', () => {
    const {host, service} = mount(null, true);
    expect(text(host.querySelector('[role="alert"]'))).toContain(page.loadFailed);
    host.querySelector('[role="alert"] app-button')!.dispatchEvent(new Event('clicked'));
    expect(service.load).toHaveBeenLastCalledWith(true);
  });
});
