import {beforeAll, describe, expect, it, vi} from 'vitest';
import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {
  ConnectorDriversPageComponent,
  driverAnchor,
  errorDetail,
} from './connector-drivers-page.component';
import {ApiService} from '../../core/services/api.service';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {UserService} from '../../core/services/user.service';
import {ConnectorDriver, ConnectorDriverMatrix} from '../../core/models/connector-driver.model';
// The API's own response for the built-in drivers
// (tests/test_connector_capability_matrix.py pins it).
import fixture from '../../core/models/fixtures/connector-drivers.json';
// The rows the managed MCP servers add where the chart installs them.
import managedFixture from '../../core/models/fixtures/connector-drivers-managed.json';
// The real catalogue, so these specs also prove the keys exist.
import en from '../../../assets/i18n/en.json';
import de from '../../../assets/i18n/de-DE.json';

const matrix = fixture as unknown as ConnectorDriverMatrix;
const managed = managedFixture as unknown as ConnectorDriverMatrix;
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

function mount(drivers: ConnectorDriver[] | null, loadFailed = false, admin = false) {
  const service = {
    drivers: signal(drivers),
    loadFailed: signal(loadFailed),
    load: vi.fn(),
  };
  const api = {
    registerConnectorDriver: vi.fn(() => of({id: 'r1'})),
    deleteConnectorDriver: vi.fn(() => of({status: 'deleted'})),
    setConnectorDriverDisabled: vi.fn(() => of({id: 'r1'})),
  };
  const users = {currentUser: signal({id: 'u1', is_admin: admin})};
  TestBed.configureTestingModule({
    imports: [
      ConnectorDriversPageComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        preloadLangs: true,
      }),
    ],
    providers: [
      {provide: ConnectorDriversService, useValue: service},
      {provide: ApiService, useValue: api},
      {provide: UserService, useValue: users},
    ],
  });
  // The primitives stay inert (their signal inputs are not wired in this
  // harness, see helm-managed-badge.component.spec.ts); their text renders.
  TestBed.overrideComponent(ConnectorDriversPageComponent, {
    set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  const fixture = TestBed.createComponent(ConnectorDriversPageComponent);
  fixture.detectChanges();
  return {fixture, host: fixture.nativeElement as HTMLElement, service, api};
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
    // In-process: no driver pod, so "none declared" would undersell it.
    expect(text(postgres.querySelector('[data-egress="declared"]'))).toContain(page.egressNoDriverPod);
    expect(text(postgres.querySelector('[data-egress="declared"]'))).not.toContain(page.egressNone);
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

  it('labels a development driver as such, not built-in and not an author\'s', () => {
    const probe: ConnectorDriver = {
      ...CUSTOM,
      name: 'srw.lease-probe/v1',
      title: 'Lease probe (development)',
      trust: {tier: 'development', trusted: false, image: null, claims_declared_by_author: false},
    };
    const {host} = mount([probe]);
    const card = host.querySelector<HTMLElement>('[data-driver="srw.lease-probe/v1"]')!;
    expect(text(card.querySelector('.driver-badges'))).toContain(page.trust.development);
    expect(text(card.querySelector('.driver-badges'))).not.toContain(page.trust.builtin);
    expect(card.querySelector('[data-claims="author"]')).toBeNull();
  });

  it("labels a managed MCP server managed, with its image, its claims SRW's", () => {
    const {host} = mount([...matrix.drivers, ...managed.drivers]);
    for (const driver of managed.drivers) {
      const server = card(host, driver.name);
      expect(text(server.querySelector('.driver-badges'))).toContain(page.trust.managed);
      expect(text(server.querySelector('.driver-badges'))).not.toContain(page.trust.custom);
      // Its image is shown where a built-in says it ships with SRW.
      expect(text(server)).toContain(driver.trust.image!);
      expect(text(server)).not.toContain(page.shipsWithSrw);
      // SRW wrote its spec and its front enforces the levels: no author's word.
      expect(server.querySelector('[data-claims="author"]')).toBeNull();
      expect(server.querySelector('.claim-source')).toBeNull();
      expect(text(server.querySelector('[data-level="ReadOnly"] .enforced-by'))).toContain("SRW's front");
    }
  });

  it('has a trust label for every tier the API sends, in both languages', () => {
    const tiers = new Set([...matrix.drivers, ...managed.drivers].map((d) => d.trust.tier));
    for (const tier of [...tiers, 'trusted', 'custom', 'development'] as const) {
      expect(en.connectorDrivers.trust[tier], tier).toBeTruthy();
      expect(de.connectorDrivers.trust[tier], tier).toBeTruthy();
    }
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

  it('shows how a hosted service driver pod is pinned, and the installation status', () => {
    const service: ConnectorDriver = {
      ...CUSTOM,
      name: 'srw.echo-service/v1',
      plane: 'service',
      egress: {
        declared: {rules: [{host: '${config.host}', ports: [443], protocol: 'tcp'}], needs_dns: null},
        enforced: {status: 'enforced', reason: 'pinned_per_pod'},
        installation: {status: 'unverified', reason: 'start_up_wait_unverified'},
      },
    };
    const {host} = mount([service]);
    const hosted = card(host, service.name);
    expect(text(hosted.querySelector('[data-egress="enforced"]'))).toContain(
      page.egressReason.pinned_per_pod,
    );
    expect(text(hosted.querySelector('[data-egress="installation"]'))).toContain(
      page.egressReason.start_up_wait_unverified,
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

  describe('registering a driver image (D6)', () => {
    it('offers the shared catalog to administrators only', () => {
      const user = mount([]).host.querySelector('[data-section="register"]')!;
      expect(text(user)).toContain(page.register.title);
      expect(text(user)).toContain(page.register.scopeAccount);
      expect(text(user)).not.toContain(page.register.scopeCatalog);
      TestBed.resetTestingModule();
      const admin = mount([], false, true).host.querySelector('[data-section="register"]')!;
      expect(text(admin)).toContain(page.register.scopeCatalog);
    });

    it('registers the image in the chosen scope and reloads the matrix', () => {
      const {fixture, service, api} = mount([], false, true);
      const component = fixture.componentInstance;
      component.image.set('  ghcr.io/acme/env:1  ');
      component.register();
      expect(api.registerConnectorDriver).toHaveBeenLastCalledWith({image: 'ghcr.io/acme/env:1'});
      expect(service.load).toHaveBeenLastCalledWith(true);
      expect(component.image()).toBe('');
      component.image.set('ghcr.io/acme/env:1');
      component.scope.set('Catalog');
      component.register();
      expect(api.registerConnectorDriver).toHaveBeenLastCalledWith({
        image: 'ghcr.io/acme/env:1',
        scope: {kind: 'Catalog', name: 'shared'},
      });
    });

    it("shows the server's reason for a refused image", () => {
      const {fixture, host, api} = mount([]);
      api.registerConnectorDriver.mockReturnValueOnce(
        throwError(() => ({status: 422, error: {detail: "driver names under srw. are SRW's own"}})),
      );
      fixture.componentInstance.image.set('ghcr.io/acme/srw:1');
      fixture.componentInstance.register();
      fixture.detectChanges();
      expect(text(host.querySelector('.register-error'))).toBe(
        "driver names under srw. are SRW's own",
      );
    });

    it('shows where a registered driver lives and deletes its registration', () => {
      const registered: ConnectorDriver = {
        ...CUSTOM,
        registration: {
          id: 'r1',
          scope: {kind: 'Account', name: 'u1'},
          image_reference: 'ghcr.io/acme/ticketing:1',
          image_digest: 'sha256:' + '1'.repeat(64),
          spec_source: 'label',
        },
      };
      const {fixture, host, api, service} = mount([registered]);
      const section = card(host, CUSTOM.name).querySelector('[data-section="registration"]')!;
      expect(text(section)).toContain(page.register.scopeKind.Account);
      expect(text(section)).toContain('sha256:' + '1'.repeat(64));
      fixture.componentInstance.remove('r1');
      expect(api.deleteConnectorDriver).toHaveBeenCalledWith('r1');
      expect(service.load).toHaveBeenLastCalledWith(true);
    });

    it('reads an error detail, else the status', () => {
      expect(errorDetail({status: 409, error: {detail: 'in use'}})).toBe('in use');
      expect(errorDetail({status: 500, error: {}})).toBe('HTTP 500');
      expect(errorDetail(null)).toBe('Request failed');
    });

    it("renders a validation 422's list of messages, and an object's message", () => {
      const list = {
        status: 422,
        error: {detail: [{loc: ['body', 'image'], msg: 'Field required'}, {msg: 'too long'}]},
      };
      expect(errorDetail(list)).toBe('Field required; too long');
      const ambiguous = {
        status: 409,
        error: {detail: {message: 'Ambiguous driver name', registrations: []}},
      };
      expect(errorDetail(ambiguous)).toBe('Ambiguous driver name');
    });

    const catalogDriver = (disabled = false, can_manage = true): ConnectorDriver => ({
      ...CUSTOM,
      registration: {
        id: 'r2',
        scope: {kind: 'Catalog', name: 'shared'},
        image_reference: 'ghcr.io/acme/ticketing:1',
        image_digest: 'sha256:' + '2'.repeat(64),
        spec_source: 'label',
        env_names: ['TICKETS_TOKEN'],
        disabled,
        can_manage,
        ...(can_manage ? {usage: {connectors: 2, live_bindings: 3}} : {}),
      },
    });

    it("offers Delete and Disable only where the server says the caller may", () => {
      const reader = card(mount([catalogDriver(false, false)], false, true).host, CUSTOM.name);
      expect(text(reader)).not.toContain(page.register.delete);
      expect(text(reader)).not.toContain(page.register.disable);
      expect(text(reader)).toContain('TICKETS_TOKEN');
      TestBed.resetTestingModule();
      const manager = card(mount([catalogDriver()]).host, CUSTOM.name);
      expect(text(manager)).toContain(page.register.delete);
      expect(text(manager)).toContain(page.register.disable);
    });

    it("hides a Project registration's buttons from the project's viewers", () => {
      const viewer: ConnectorDriver = {
        ...CUSTOM,
        registration: {
          id: 'r3',
          scope: {kind: 'Project', name: 'p1'},
          image_reference: 'ghcr.io/acme/ticketing:1',
          image_digest: 'sha256:' + '3'.repeat(64),
          spec_source: 'label',
          can_manage: false,
        },
      };
      const section = card(mount([viewer]).host, CUSTOM.name);
      expect(text(section)).not.toContain(page.register.disable);
      expect(text(section)).not.toContain(page.register.delete);
    });

    it('disables and enables a registration, and shows it disabled', () => {
      const {fixture, host, api, service} = mount([catalogDriver(true)], false, true);
      const section = card(host, CUSTOM.name);
      expect(section.querySelector('[data-registration="disabled"]')).not.toBeNull();
      expect(text(section)).toContain(page.register.enable);
      const confirm = vi.spyOn(window, 'confirm');
      fixture.componentInstance.setDisabled(catalogDriver(true).registration!, false);
      // Enabling revokes nothing: no question.
      expect(confirm).not.toHaveBeenCalled();
      expect(api.setConnectorDriverDisabled).toHaveBeenCalledWith('r2', false);
      expect(service.load).toHaveBeenLastCalledWith(true);
      confirm.mockRestore();
    });

    it('asks before a Disable, naming what it revokes', () => {
      const {fixture, api} = mount([catalogDriver()]);
      const registration = catalogDriver().registration!;
      const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
      fixture.componentInstance.setDisabled(registration, true);
      expect(api.setConnectorDriverDisabled).not.toHaveBeenCalled();
      const question = confirm.mock.calls[0][0] as string;
      expect(question).toContain('ghcr.io/acme/ticketing:1');
      expect(question).toContain('3 live binding');
      expect(question).toContain('2 connector');
      confirm.mockReturnValue(true);
      fixture.componentInstance.setDisabled(registration, true);
      expect(api.setConnectorDriverDisabled).toHaveBeenCalledWith('r2', true);
      confirm.mockRestore();
    });

    it("shows a refused Disable or Delete on the registration's own card", () => {
      const {fixture, host, api} = mount([catalogDriver()]);
      api.deleteConnectorDriver.mockReturnValueOnce(
        throwError(() => ({status: 409, error: {detail: '1 binding(s) of this driver are not revoked yet'}})),
      );
      fixture.componentInstance.remove('r2');
      fixture.detectChanges();
      const section = card(host, CUSTOM.name);
      expect(text(section.querySelector('[data-card-error="r2"]'))).toBe(
        '1 binding(s) of this driver are not revoked yet',
      );
      // Not in the Register section.
      expect(host.querySelector('[data-section="register"] .register-error')).toBeNull();
    });
  });
});
