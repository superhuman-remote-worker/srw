import {beforeAll, describe, expect, it} from 'vitest';
import {ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {DatasourcesGroupComponent} from './datasources-group.component';
import type {Datasource} from '../../core/models/api.model';

function ds(id: string, name: string, type: string, extra: Partial<Datasource> = {}): Datasource {
  return {
    id, name, type, description: null, connection_url: null, cli_hint: null,
    default_branch: null, job_id: null, created_at: '', updated_at: '',
    ...extra,
  } as Datasource;
}

function mount(list: Datasource[]) {
  TestBed.configureTestingModule({
    imports: [
      DatasourcesGroupComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
  });
  const fixture = TestBed.createComponent(DatasourcesGroupComponent);
  const stub = (name: string, value: unknown) =>
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  stub('datasources', list);
  stub('searchable', false);
  stub('showHeader', false);
  stub('datasourceDefaultsEnabled', true);
  stub('isLiteBackend', false);
  fixture.detectChanges();
  return fixture.nativeElement as HTMLElement;
}

describe('DatasourcesGroupComponent: a registered driver says what it sets (D6)', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('lists the variables a registered driver declares before it is attached', () => {
    const host = mount([
      ds('img', 'Ticketing', 'image_driver', {driver_env_names: ['TICKETS_TOKEN', 'TICKETS_URL']}),
      ds('pg', 'Analytics warehouse', 'postgresql'),
    ]);
    const lines = Array.from(host.querySelectorAll('[data-driver-env]')).map((el) =>
      (el.textContent ?? '').replace(/\s+/g, ' ').trim(),
    );
    expect(lines).toEqual([`${en.datasources.driverEnvNames} TICKETS_TOKEN, TICKETS_URL`]);
    expect(host.textContent).toContain(en.datasources.filter.image_driver);
  });
});
