import {beforeAll, describe, expect, it} from 'vitest';
import {ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {DatasourcesGroupComponent} from './datasources-group.component';
import type {EligibleDatasource} from '../../core/models/api.model';

/**
 * The project's own connectors in the picker (main_cloud_as_connectors.md,
 * "What people see"): preselected and removable whatever the rollout gate
 * says, named for what they are, with their access level when the row
 * carries one, and an empty selection said in words.
 */
function ds(id: string, type: string, extra: Partial<EligibleDatasource> = {}): EligibleDatasource {
  return {
    id, name: id, type, description: null, connection_url: null, cli_hint: null,
    default_branch: null, job_id: null, created_at: '', updated_at: '',
    default_selected: false,
    ...extra,
  } as EligibleDatasource;
}

const FOLDER = ds('folder', 'cloud_folder', {
  managed_key: 'project-cloud:11111111-1111-1111-1111-111111111111',
  default_selected: true,
});
const KB = ds('kb', 'kb', {
  config: {native_project_id: '11111111-1111-1111-1111-111111111111'},
  default_selected: true,
});
const SHARED = ds('shared', 'postgresql', {default_selected: true});

function mount(
  rows: EligibleDatasource[],
  options: {defaults?: boolean; initial?: string[] | null; locked?: string[]} = {},
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
  stub('datasourceDefaultsEnabled', options.defaults ?? false);
  stub('initialSelectedIds', options.initial ?? null);
  stub('lockedIds', options.locked ?? []);
  fixture.detectChanges();
  return fixture;
}

function host(fixture: {nativeElement: unknown}): HTMLElement {
  return fixture.nativeElement as HTMLElement;
}

function row(fixture: {nativeElement: unknown}, name: string): HTMLElement {
  const found = Array.from(host(fixture).querySelectorAll<HTMLElement>('.ds-option')).find(
    (el) => (el.querySelector('.ds-name')?.textContent ?? '').trim() === name,
  );
  if (!found) throw new Error(`no row ${name}`);
  return found;
}

function badges(el: HTMLElement): string[] {
  return Array.from(el.querySelectorAll('.ds-type-badge')).map((b) => (b.textContent ?? '').trim());
}

describe("DatasourcesGroupComponent a project's own connectors", () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('preselects them with the rollout gate off, and only them', () => {
    const fixture = mount([FOLDER, KB, SHARED]);
    const c = fixture.componentInstance;
    expect(c.getSelectedIds()).toEqual(['folder', 'kb']);
    expect((row(fixture, 'folder').querySelector('input') as HTMLInputElement).checked).toBe(true);
    expect((row(fixture, 'shared').querySelector('input') as HTMLInputElement).checked).toBe(false);
    // Untouched, nothing differs from the defaults.
    expect(c.modifiedCount()).toBe(0);
  });

  it('keeps them removable: a click takes one off', () => {
    const fixture = mount([FOLDER, KB, SHARED]);
    const c = fixture.componentInstance;
    const box = row(fixture, 'folder').querySelector('input') as HTMLInputElement;
    expect(box.disabled).toBe(false);
    box.click();
    fixture.detectChanges();
    expect(c.getSelectedIds()).toEqual(['kb']);
    expect(c.modifiedCount()).toBe(1);
  });

  it('says so when nothing is attached, the empty list a create then sends', () => {
    const fixture = mount([FOLDER, KB, SHARED]);
    const c = fixture.componentInstance;
    expect(host(fixture).querySelector('[data-nothing-attached]')).toBeNull();
    c.toggle('folder');
    c.toggle('kb');
    fixture.detectChanges();
    expect(c.getSelectedIds()).toEqual([]);
    expect(host(fixture).querySelector('[data-nothing-attached] span')?.textContent?.trim()).toBe(
      en.agentSettings.datasources.nothingAttached,
    );
    expect(host(fixture).querySelector('.ds-count')?.textContent?.trim()).toBe('0 of 3 attached');
  });

  it('shows no such note where no connector of the project is on offer', () => {
    const fixture = mount([SHARED]);
    expect(fixture.componentInstance.getSelectedIds()).toEqual([]);
    expect(host(fixture).querySelector('[data-nothing-attached]')).toBeNull();
  });

  it('names a platform-owned row for what it is', () => {
    const outbox = ds('outbox', 'cloud_outbox', {
      managed_key: 'cloud-outbox:22222222-2222-2222-2222-222222222222',
    });
    const unknown = ds('later', 'cloud_folder', {
      managed_key: 'some-later-kind:22222222-2222-2222-2222-222222222222',
    });
    const fixture = mount([FOLDER, KB, outbox, unknown, SHARED]);
    expect(badges(row(fixture, 'folder'))[0]).toBe(en.datasources.platform.kind.projectCloud);
    expect(badges(row(fixture, 'kb'))[0]).toBe(en.datasources.platform.kind.projectKb);
    expect(badges(row(fixture, 'outbox'))[0]).toBe(en.datasources.platform.kind.cloudOutbox);
    // A kind this cockpit does not know keeps its type's label.
    expect(badges(row(fixture, 'later'))[0]).toBe(en.datasources.filter.cloud_folder);
    expect(badges(row(fixture, 'shared'))[0]).toBe(en.datasources.filter.postgresql);
  });

  it("shows the access level a row carries, and none when it carries none", () => {
    const access = (level: string) => ({
      levels: [
        {id: 'read_only', rank: 10, writes: 'none' as const, offered: true},
        {id: 'protected', rank: 20, writes: 'reviewed' as const, offered: true},
        {id: 'read_write', rank: 30, writes: 'direct' as const, offered: true},
        {id: 'some_new_level', rank: 40, writes: 'direct' as const, offered: true},
      ],
      default: level,
    });
    const fixture = mount([
      {...FOLDER, id: 'protected', name: 'protected', access: access('protected')},
      {...FOLDER, id: 'rw', name: 'rw', access: access('read_write')},
      {...FOLDER, id: 'odd', name: 'odd', access: access('some_new_level')},
      FOLDER,
    ]);
    const level = (name: string) => row(fixture, name).querySelector('[data-access-level]');
    expect(level('protected')?.textContent?.trim()).toBe(en.datasources.accessLevel.protected);
    expect(level('protected')?.classList.contains('ds-rw-badge')).toBe(false);
    expect(level('rw')?.textContent?.trim()).toBe(en.datasources.accessLevel.read_write);
    // A level that writes straight through takes the read-write tone.
    expect(level('rw')?.classList.contains('ds-rw-badge')).toBe(true);
    // A level this cockpit does not name shows its id.
    expect(level('odd')?.textContent?.trim()).toBe('some_new_level');
    expect(level('folder')).toBeNull();
  });

  it('says why a locked row cannot change during a session', () => {
    const fixture = mount([FOLDER, KB], {initial: ['folder', 'kb'], locked: ['folder', 'kb']});
    const hint = (name: string) =>
      row(fixture, name).querySelector('[data-locked-hint]')?.textContent?.trim();
    expect(hint('folder')).toBe(en.agentSettings.datasources.lockedNextStart);
    expect(hint('kb')).toBe(en.agentSettings.datasources.lockedLive);
  });
});
