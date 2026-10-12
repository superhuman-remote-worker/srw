import {beforeAll, describe, expect, it} from 'vitest';
import {ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {
  accessOriginKey,
  accessRefusalKey,
  DatasourceAccessMode,
  DatasourcesGroupComponent,
} from './datasources-group.component';
import type {ConnectorAccessChoice, EligibleDatasource} from '../../core/models/api.model';

/**
 * A cloud folder's access level in the picker (main cloud slice 5): New
 * Session chooses it among the levels the eligible list offers on the
 * picked tier, a job shows the level its project link binds, and a live
 * session shows the recorded level, locked.
 */
const LEVELS = [
  {id: 'read_only', rank: 10, writes: 'none' as const, offered: true, reason: null, detail: null},
  {id: 'protected', rank: 20, writes: 'reviewed' as const, offered: true, reason: null, detail: null},
  {id: 'read_write', rank: 30, writes: 'direct' as const, offered: true, reason: null, detail: null},
];

function access(over: Partial<ConnectorAccessChoice> = {}): ConnectorAccessChoice {
  return {
    levels: LEVELS,
    default: 'read_write',
    default_origin: 'link_default',
    default_reason: null,
    ceiling: null,
    ...over,
  };
}

function ds(id: string, type: string, extra: Partial<EligibleDatasource> = {}): EligibleDatasource {
  return {
    id, name: id, type, description: null, connection_url: null, cli_hint: null,
    default_branch: null, job_id: null, created_at: '', updated_at: '',
    default_selected: true,
    managed_key: `project-cloud:${id}`,
    ...extra,
  } as EligibleDatasource;
}

function mount(
  rows: EligibleDatasource[],
  options: {mode?: DatasourceAccessMode; initial?: string[] | null; initialAccess?: Record<string, string> | null; locked?: string[]} = {},
) {
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
  // Signal inputs cannot be set through setInput() in this pipeline; see
  // datasources-group.search.spec.ts.
  const stub = (name: string, value: unknown) =>
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  stub('datasources', rows);
  stub('searchable', true);
  stub('showHeader', false);
  stub('accessMode', options.mode ?? 'choose');
  stub('initialSelectedIds', options.initial ?? null);
  stub('initialAccess', options.initialAccess ?? null);
  stub('lockedIds', options.locked ?? []);
  fixture.detectChanges();
  return fixture;
}

function row(fixture: {nativeElement: unknown}, name: string): HTMLElement {
  const found = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>('.ds-option'))
    .find((el) => (el.querySelector('.ds-name')?.textContent ?? '').trim() === name);
  if (!found) throw new Error(`no row ${name}`);
  return found;
}

function select(fixture: {nativeElement: unknown}, name: string): HTMLSelectElement | null {
  return row(fixture, name).querySelector<HTMLSelectElement>('[data-access-select]');
}

function options(el: HTMLSelectElement): Array<{value: string; text: string; disabled: boolean; selected: boolean}> {
  return Array.from(el.options).map((o) => ({
    value: o.value,
    text: (o.textContent ?? '').replace(/\s+/g, ' ').trim(),
    disabled: o.disabled,
    selected: o.selected,
  }));
}

const L = en.datasources.accessLevel;
const A = en.agentSettings.datasources.access;

describe('DatasourcesGroupComponent access levels', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it("offers a selected folder's levels, its default marked with where it came from", () => {
    const fixture = mount([
      ds('folder', 'cloud_folder', {
        access: access({
          levels: [
            ...LEVELS.slice(0, 1),
            {...LEVELS[1], offered: false, reason: 'level_not_offered', detail: 'Cloud folder does not offer protected here'},
            LEVELS[2],
          ],
        }),
      }),
    ]);
    const el = select(fixture, 'folder')!;
    expect(el).not.toBeNull();
    expect(options(el)).toEqual([
      {value: 'read_only', text: L.read_only, disabled: false, selected: false},
      {value: 'protected', text: `${L.protected} · ${A.refused.notOffered}`, disabled: true, selected: false},
      {value: 'read_write', text: `${L.read_write} · ${A.origin.projectDefault}`, disabled: false, selected: true},
    ]);
    // The server's own words stay a tooltip.
    expect(el.options[1].title).toBe('Cloud folder does not offer protected here');
  });

  it('sends only the levels the user chose, for selected rows', () => {
    const fixture = mount([ds('folder', 'cloud_folder', {access: access()}), ds('pg', 'postgresql', {managed_key: null, default_selected: false})]);
    const c = fixture.componentInstance;
    expect(c.getSelectedDatasourceAccess()).toEqual({});
    expect(c.modifiedCount()).toBe(0);
    c.chooseAccess('folder', 'read_only');
    fixture.detectChanges();
    expect(c.getSelectedDatasourceAccess()).toEqual({folder: 'read_only'});
    expect(c.modifiedCount()).toBe(1);
    expect(select(fixture, 'folder')!.value).toBe('read_only');
    // Back to the default: nothing to send, the server binds it anyway.
    c.chooseAccess('folder', 'read_write');
    expect(c.getSelectedDatasourceAccess()).toEqual({});
    // A level that is not offered, or a row without levels, is never taken.
    c.chooseAccess('folder', 'owner');
    c.chooseAccess('pg', 'read_only');
    expect(c.getSelectedDatasourceAccess()).toEqual({});
    // A removed row sends no level.
    c.chooseAccess('folder', 'protected');
    c.toggle('folder');
    expect(c.getSelectedDatasourceAccess()).toEqual({});
  });

  it('marks a lowered default with why, and a folder no level fits as not mounted', () => {
    const fixture = mount([
      ds('viewer', 'cloud_folder', {
        default_selected: false,
        access: access({
          default: 'read_only',
          default_origin: 'clamped',
          ceiling: 'read_only',
          levels: [LEVELS[0], {...LEVELS[1], offered: false, reason: 'above_ceiling'}, {...LEVELS[2], offered: false, reason: 'above_ceiling'}],
        }),
      }),
      ds('none', 'cloud_folder', {default: undefined, access: access({default: null, default_origin: null})} as Partial<EligibleDatasource>),
    ]);
    const c = fixture.componentInstance;
    c.toggle('viewer');
    fixture.detectChanges();
    // A viewer has one level left: shown, with why it was lowered.
    expect(select(fixture, 'viewer')).toBeNull();
    const badge = row(fixture, 'viewer').querySelector('[data-access-level]')!;
    expect(badge.textContent?.trim()).toBe(L.read_only);
    expect(badge.getAttribute('title')).toBe(A.origin.loweredRole);
    const none = options(select(fixture, 'none')!);
    expect(none[0]).toEqual({value: '', text: A.notMounted, disabled: true, selected: true});
  });

  it('protects one folder at most, and says the others stay out beside it', () => {
    const fixture = mount([
      ds('a', 'cloud_folder', {access: access()}),
      ds('b', 'cloud_folder', {access: access()}),
      ds('outbox', 'cloud_outbox', {
        managed_key: 'cloud-outbox:u1',
        access: access({levels: [LEVELS[2]], default: 'read_write', default_origin: 'driver_default'}),
      }),
      ds('kb', 'kb', {managed_key: 'project-kb:p1'}),
    ]);
    const c = fixture.componentInstance;
    c.chooseAccess('a', 'protected');
    fixture.detectChanges();
    const beside = (name: string) => row(fixture, name).querySelector('[data-beside-protected]');
    expect(beside('a')).toBeNull();
    expect(beside('b')?.textContent?.trim()).toBe(A.besideProtected);
    expect(beside('outbox')?.textContent?.trim()).toBe(A.besideProtected);
    // A knowledge base is no cloud mount: it stays.
    expect(beside('kb')).toBeNull();
    const protectedOption = options(select(fixture, 'b')!).find((o) => o.value === 'protected')!;
    expect(protectedOption.disabled).toBe(true);
    expect(protectedOption.text).toBe(`${L.protected} · ${A.refused.oneProtected}`);
    c.chooseAccess('b', 'protected');
    expect(c.getSelectedDatasourceAccess()).toEqual({a: 'protected'});
  });

  it('shows the level on a job form, with no select and nothing to send', () => {
    const fixture = mount([ds('copy', 'cloud_folder_checkout', {
      managed_key: 'project-cloud-checkout:p1',
      access: access({
        levels: [
          {id: 'read_only', rank: 10, writes: 'none', offered: true},
          {id: 'reviewed_write_back', rank: 20, writes: 'reviewed', offered: true},
        ],
        default: 'reviewed_write_back',
      }),
    })], {mode: 'show'});
    const c = fixture.componentInstance;
    expect(select(fixture, 'copy')).toBeNull();
    const badge = row(fixture, 'copy').querySelector('[data-access-level]')!;
    expect(badge.textContent?.trim()).toBe(L.reviewed_write_back);
    expect(badge.getAttribute('title')).toBe(A.origin.projectDefault);
    c.chooseAccess('copy', 'read_only');
    expect(c.getSelectedDatasourceAccess()).toEqual({});
  });

  it("shows a live session's recorded level, locked", () => {
    const fixture = mount([ds('folder', 'cloud_folder', {access: access()})], {
      mode: 'locked',
      initial: ['folder'],
      initialAccess: {folder: 'protected'},
      locked: ['folder'],
    });
    const el = select(fixture, 'folder')!;
    expect(el.disabled).toBe(true);
    expect(el.value).toBe('protected');
    expect(fixture.componentInstance.getSelectedDatasourceAccess()).toEqual({});
  });

  it('drops a choice the list stops offering when the tier moves', () => {
    const rows = [ds('folder', 'cloud_folder', {access: access()})];
    const fixture = mount(rows);
    const c = fixture.componentInstance;
    c.chooseAccess('folder', 'protected');
    expect(c.getSelectedDatasourceAccess()).toEqual({folder: 'protected'});
    // Re-read for a VM: protected is the container tier's.
    rows[0] = ds('folder', 'cloud_folder', {
      access: access({levels: [LEVELS[0], {...LEVELS[1], offered: false, reason: 'level_not_offered'}, LEVELS[2]]}),
    });
    Object.defineProperty(c, 'datasources', {value: () => [...rows]});
    expect(c.accessLevel(rows[0])).toBe('read_write');
    expect(c.getSelectedDatasourceAccess()).toEqual({});
  });

  it('shows My cloud as full access, and says why there is no narrower level', () => {
    const fixture = mount([
      ds('root', 'cloud_folder', {
        managed_key: 'user-cloud-root:u1',
        access: access({
          levels: [
            {...LEVELS[0], offered: false, reason: 'level_not_offered'},
            {...LEVELS[1], offered: false, reason: 'level_not_offered'},
            LEVELS[2],
          ],
          default: 'read_write',
          default_origin: 'driver_default',
        }),
      }),
    ]);
    // One level offered: no select to choose from.
    expect(select(fixture, 'root')).toBeNull();
    const badge = row(fixture, 'root').querySelector('[data-access-level]')!;
    expect(badge.textContent?.trim()).toBe(A.fullAccess);
    expect(badge.getAttribute('title')).toBe(A.fullAccessHint);
    expect(A.fullAccessHint).toContain('read-only');
    expect(row(fixture, 'root').querySelector('.ds-type-badge')?.textContent?.trim()).toBe(
      en.datasources.platform.kind.userCloudRoot,
    );
  });

  it('shows an unselected row its default level, not a select', () => {
    const fixture = mount([ds('folder', 'cloud_folder', {default_selected: false, access: access()})]);
    expect(select(fixture, 'folder')).toBeNull();
    expect(row(fixture, 'folder').querySelector('[data-access-level]')?.textContent?.trim()).toBe(L.read_write);
  });

  it('names each origin and refusal with a key both catalogues carry', () => {
    expect(accessOriginKey(access({default_origin: 'link_default'}))).toBe('agentSettings.datasources.access.origin.projectDefault');
    expect(accessOriginKey(access({default_origin: 'driver_default'}))).toBe('agentSettings.datasources.access.origin.default');
    expect(accessOriginKey(access({default_origin: 'clamped'}))).toBe('agentSettings.datasources.access.origin.lowered');
    expect(accessOriginKey(access({default_origin: 'clamped', ceiling: 'read_only'}))).toBe('agentSettings.datasources.access.origin.loweredRole');
    expect(accessRefusalKey(LEVELS[0])).toBeNull();
    expect(accessRefusalKey({...LEVELS[0], offered: false, reason: 'read_only_unavailable'})).toBe(
      'agentSettings.datasources.access.refused.readOnlyUnavailable',
    );
    // An unknown code reads as not offered.
    expect(accessRefusalKey({...LEVELS[0], offered: false, reason: 'later'})).toBe(
      'agentSettings.datasources.access.refused.notOffered',
    );
  });
});
