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

const LIST = [
  ds('kb', 'SRW Platform knowledge base', 'kb', {default_selected: true} as Partial<Datasource>),
  ds('pg', 'Analytics warehouse', 'postgresql'),
  ds('mail', 'Support inbox', 'email', {description: 'Customer tickets'}),
  ds('repo', 'superhuman-remote-worker', 'repository', {default_selected: true} as Partial<Datasource>),
];

function mount(options: {lite?: boolean} = {}) {
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
  stub('datasources', LIST);
  stub('searchable', true);
  stub('showHeader', false);
  stub('datasourceDefaultsEnabled', true);
  stub('isLiteBackend', options.lite ?? false);
  fixture.detectChanges();
  return fixture;
}

function rowNames(fixture: {nativeElement: unknown}): string[] {
  return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.ds-name'))
    .map((el) => (el.textContent ?? '').trim());
}

describe('DatasourcesGroupComponent search and filter', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('matches name, type and description, case-insensitively', () => {
    const fixture = mount();
    const c = fixture.componentInstance;
    c.query.set('WAREHOUSE');
    expect(c.visibleDatasources().map((d) => d.id)).toEqual(['pg']);
    c.query.set('email');
    expect(c.visibleDatasources().map((d) => d.id)).toEqual(['mail']);
    c.query.set('tickets');
    expect(c.visibleDatasources().map((d) => d.id)).toEqual(['mail']);
  });

  it('a filtered-out row keeps its selection: search is presentation only', () => {
    const fixture = mount();
    const c = fixture.componentInstance;
    const before = c.getSelectedIds();
    expect(before.sort()).toEqual(['kb', 'repo']);
    c.query.set('analytics');
    expect(c.getSelectedIds().sort()).toEqual(before.sort());
  });

  it('Attached shows only what would be attached', () => {
    const fixture = mount();
    const c = fixture.componentInstance;
    c.onlyAttached.set(true);
    expect(c.visibleDatasources().map((d) => d.id).sort()).toEqual(['kb', 'repo']);
  });

  it('selectedList follows the lite-tier exclusion, like getSelectedIds', () => {
    const fixture = mount({lite: true});
    expect(fixture.componentInstance.selectedList().map((d) => d.id)).toEqual(['kb']);
  });

  it('renders the search box and an empty-match message', () => {
    const fixture = mount();
    const el = fixture.nativeElement as HTMLElement;
    expect(el.querySelector('input[type="search"]')).not.toBeNull();
    expect(rowNames(fixture)).toHaveLength(4);
    fixture.componentInstance.query.set('nothing-like-this');
    fixture.detectChanges();
    expect(rowNames(fixture)).toHaveLength(0);
    expect(el.querySelector('.ds-empty')?.textContent).toContain('nothing-like-this');
  });
});
