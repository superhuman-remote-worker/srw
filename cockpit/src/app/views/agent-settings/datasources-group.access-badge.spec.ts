import {beforeAll, describe, expect, it, vi} from 'vitest';
import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {DatasourcesGroupComponent} from './datasources-group.component';
import {Datasource, DatasourceType} from '../../core/models/api.model';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {
  ConnectorDriver,
  ConnectorDriverMatrix,
  driverForType,
} from '../../core/models/connector-driver.model';
import fixture from '../../core/models/fixtures/connector-drivers.json';

const DRIVERS = (fixture as unknown as ConnectorDriverMatrix).drivers;

function ds(id: string, type: string, readOnly: boolean, isGlobal = true): Datasource {
  return {
    id,
    name: id,
    description: null,
    type: type as DatasourceType,
    connection_url: null,
    cli_hint: null,
    default_branch: null,
    job_id: null,
    created_at: '',
    updated_at: '',
    is_global: isGlobal,
    read_only: readOnly,
  };
}

function mount(rows: Datasource[], drivers: ConnectorDriver[] | null, lite = false) {
  const service = {
    drivers: signal(drivers),
    load: vi.fn(),
    forType: (type: string) => driverForType(drivers, type),
  };
  TestBed.configureTestingModule({
    imports: [
      DatasourcesGroupComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
    providers: [{provide: ConnectorDriversService, useValue: service}],
  });
  const fixture = TestBed.createComponent(DatasourcesGroupComponent);
  // Signal inputs cannot be set through setInput() in this pipeline; see
  // datasources-group.search.spec.ts.
  Object.defineProperty(fixture.componentInstance, 'datasources', {value: () => rows});
  Object.defineProperty(fixture.componentInstance, 'isLiteBackend', {value: () => lite});
  fixture.detectChanges();
  return {fixture, service};
}

function badges(fixture: {nativeElement: unknown}): Record<string, string> {
  const out: Record<string, string> = {};
  for (const row of Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.ds-option'))) {
    const name = (row.querySelector('.ds-name')?.textContent ?? '').trim();
    const all = Array.from(row.querySelectorAll('.ds-type-badge')).map((b) => (b.textContent ?? '').trim());
    out[name] = all[1] ?? '';
  }
  return out;
}

describe('DatasourcesGroupComponent public access badge', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it("shows a driver's only level over a stored flag that contradicts it", () => {
    const rows = [ds('mcp', 'mcp', true), ds('kb', 'kb', false), ds('pg', 'postgresql', true), ds('pg-rw', 'postgresql', false)];
    const {fixture, service} = mount(rows, DRIVERS);
    expect(service.load).toHaveBeenCalled();
    expect(badges(fixture)).toEqual({
      mcp: en.datasources.table.badgeRw,
      kb: en.datasources.table.badgeRo,
      pg: en.datasources.table.badgeRo,
      'pg-rw': en.datasources.table.badgeRw,
    });
  });

  it('reads the stored flag until the matrix loads, and loads it only for public rows', () => {
    const {fixture} = mount([ds('mcp', 'mcp', true)], null);
    expect(badges(fixture)).toEqual({mcp: en.datasources.table.badgeRo});
    TestBed.resetTestingModule();
    const {service} = mount([ds('pg', 'postgresql', true, false)], null);
    expect(service.load).not.toHaveBeenCalled();
  });
});

describe('DatasourcesGroupComponent lite tier from the matrix', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it("holds back what the driver's backends leave out, and loads the matrix to know", () => {
    const rows = [ds('env', 'generic', true, false), ds('pg', 'postgresql', true, false)];
    const drivers = DRIVERS.map((driver) =>
      driver.legacy_type === 'generic'
        ? {...driver, supported_backends: ['none', 'sandbox', 'virtual', 'vm']}
        : driver,
    );
    const {fixture, service} = mount(rows, drivers, true);
    expect(service.load).toHaveBeenCalled();
    const c = fixture.componentInstance;
    expect(rows.map((row) => c.isLiteExcluded(row))).toEqual([false, false]);
  });

  it('keeps the built-in rule before the matrix loads', () => {
    const rows = [ds('env', 'generic', true, false)];
    const {fixture} = mount(rows, null, true);
    expect(fixture.componentInstance.isLiteExcluded(rows[0])).toBe(true);
  });
});
